#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

//! Provides a data structure for tracking random-access writes to a buffer.

use std::mem;

use bytes::{BufMut, BytesMut};

use thiserror::Error;

/// The error type for writing to the `BytesQuilt`.
#[derive(Copy, Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// Attempted to write past the end of the current buffer.
    #[error("Not enough space in buffer segment")]
    NotEnoughSpace,

    /// Attempted to write more data than would fit into the missing segment.
    #[error("Would overwrite previously received data")]
    WouldOverwrite,
}

/// A byte buffer that tracks the locations of random-access writes.
#[derive(Debug)]
pub struct BytesQuilt {
    tail_offset: usize,
    segments: Vec<Segment>,
    buffer_tail: BytesMut,
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum Status {
    Missing,
    Received,
}

#[derive(Clone, Debug, PartialEq)]
struct Segment {
    status: Status,
    offset: usize,
    buffer: BytesMut,
}

/// A description of a segment in the buffer that hasn't been written to.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MissingSegment {
    offset: usize,
    length: usize,
}

impl Default for BytesQuilt {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesQuilt {
    /// Creates a new `BytesQuilt` with default capacity.
    pub fn new() -> Self {
        Self {
            tail_offset: 0,
            segments: Vec::new(),
            buffer_tail: BytesMut::new(),
        }
    }

    /// Creates a new `BytesQuilt` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tail_offset: 0,
            segments: Vec::new(),
            buffer_tail: BytesMut::with_capacity(capacity),
        }
    }

    fn write_offset_at_index(
        &mut self,
        index: usize,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), Error> {
        use std::cmp::Ordering;
        let segment = &mut self.segments[index];
        if segment.status == Status::Received {
            return Err(Error::WouldOverwrite);
        }
        match segment.buffer.capacity().cmp(&bytes.len()) {
            // TODO[ZS 2023-08-25]: This probably shouldn't even be an error,
            // we should just grow the buffer.
            Ordering::Less => return Err(Error::NotEnoughSpace),
            Ordering::Equal => {
                segment.status = Status::Received;
                segment.buffer.put(bytes);
            }
            Ordering::Greater => {
                segment.status = Status::Received;
                segment.buffer.put(bytes);
                let new_relative_offset = segment.buffer.len();
                let remaining_segment = segment.buffer.split_off(new_relative_offset);
                self.segments.insert(
                    index + 1,
                    Segment::missing(offset + new_relative_offset, remaining_segment),
                );
            }
        };
        Ok(())
    }

    /// Transfer bytes into `self` from `src` at `offset`.
    ///
    /// The `offset` is given from the beginning of the buffer.
    pub fn put_at(&mut self, offset: usize, src: &[u8]) -> Result<Option<MissingSegment>, Error> {
        let mut missing_segment = None;
        debug_assert!(
            self.segments
                .first()
                .map(|segment| segment.offset == 0)
                .unwrap_or(true),
            "first segment offset should be zero, found {:?}",
            self.segments.first()
        );
        if self.tail_offset > offset {
            // We should have a missing segment that this offset can write into
            match self
                .segments
                .binary_search_by_key(&offset, |segment| segment.offset)
            {
                Ok(index) => {
                    self.write_offset_at_index(index, offset, src)?;
                }
                Err(index) => {
                    // This indexing might be safe because the first
                    // entry in the segments vec should always start
                    // with `offset = 0`
                    let segment = &mut self.segments[index - 1];
                    let to_write_buffer = segment.buffer.split_off(offset - segment.offset);
                    let segment = Segment::missing(offset, to_write_buffer);
                    self.segments.insert(index, segment);
                    self.write_offset_at_index(index, offset, src)?;
                }
            };
            return Ok(None);
        } else if self.tail_offset + self.buffer_tail.len() < offset {
            if !self.buffer_tail.is_empty() {
                let head_offset = self.tail_offset;
                let head_received_bytes = self.buffer_tail.split();
                self.tail_offset += head_received_bytes.len();
                self.segments
                    .push(Segment::received(head_offset, head_received_bytes));
            }

            let head_offset = self.tail_offset;
            self.tail_offset = offset;

            let tail_bytes = self.buffer_tail.split_off(offset - head_offset);
            let head_bytes = mem::replace(&mut self.buffer_tail, tail_bytes);

            // This is true because of the conditional split above to
            // identify and store a received segment
            debug_assert!(head_bytes.is_empty());
            let segment = Segment::missing(head_offset, head_bytes);
            missing_segment = segment.missing_segment();
            self.segments.push(segment);
        } else if self.tail_offset == offset && !self.buffer_tail.is_empty() {
            // Supposed to write at beginning of tail, but tail is not empty!
            return Err(Error::WouldOverwrite);
        }
        self.buffer_tail.put(src);
        Ok(missing_segment)
    }

    /// An iterator over each `MissingSegment` in the `BytesQuilt`.
    pub fn missing_segments(&self) -> impl '_ + Iterator<Item = MissingSegment> {
        self.segments.iter().filter_map(Segment::missing_segment)
    }

    /// Reassemble the inner `BytesMut` and return it.
    pub fn into_inner(self) -> BytesMut {
        let mut segments = self.segments.into_iter();
        if let Some(segment) = segments.next() {
            // TODO[ZS 2023-08-25]: initialize these unwritten
            // sections with zeroes.
            debug_assert!(
                !segment.is_missing(),
                "a segment at offset {} of size {} is missing",
                segment.offset,
                segment.buffer.len(),
            );
            let mut buffer: BytesMut = segment.buffer;
            for segment in segments {
                debug_assert!(
                    !segment.is_missing(),
                    "a segment at offset {} of size {} is missing",
                    segment.offset,
                    segment.buffer.len(),
                );
                buffer.unsplit(segment.buffer);
            }
            buffer.unsplit(self.buffer_tail);
            return buffer;
        }
        self.buffer_tail
    }
}

impl Segment {
    fn missing(offset: usize, buffer: BytesMut) -> Self {
        Self {
            status: Status::Missing,
            offset,
            buffer,
        }
    }

    fn received(offset: usize, buffer: BytesMut) -> Self {
        Self {
            status: Status::Received,
            offset,
            buffer,
        }
    }

    fn is_missing(&self) -> bool {
        self.status == Status::Missing
    }

    fn missing_segment(&self) -> Option<MissingSegment> {
        match self.status {
            Status::Missing => Some(MissingSegment {
                offset: self.offset,
                length: self.buffer.capacity(),
            }),
            Status::Received => None,
        }
    }
}

impl MissingSegment {
    /// Returns an iterator of all the absolute offsets for byte
    /// segments of a specific size that can fit within this
    /// `MissingSegment`.
    pub fn offsets_for(self, frame_size: usize) -> impl Iterator<Item = usize> {
        let offset = self.offset;
        let number_of_frames = self.length / frame_size;
        (0..number_of_frames).map(move |index| (index * frame_size) + offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod missing_segment {
        use super::*;

        #[test]
        fn one_offset_missing() {
            let segment = MissingSegment {
                offset: 0,
                length: 10,
            };
            assert_eq!(&[0][..], segment.offsets_for(10).collect::<Vec<_>>());
            let segment = MissingSegment {
                offset: 10,
                length: 10,
            };
            assert_eq!(&[10][..], segment.offsets_for(10).collect::<Vec<_>>());
        }

        #[test]
        fn two_offsets_missing() {
            let segment = MissingSegment {
                offset: 0,
                length: 10,
            };
            assert_eq!(&[0, 5][..], segment.offsets_for(5).collect::<Vec<_>>());
            let segment = MissingSegment {
                offset: 10,
                length: 10,
            };
            assert_eq!(&[10, 15][..], segment.offsets_for(5).collect::<Vec<_>>());
        }

        #[test]
        fn many_offsets_missing() {
            let segment = MissingSegment {
                offset: 5,
                length: 10,
            };
            assert_eq!(
                &[5, 6, 7, 8, 9, 10, 11, 12, 13, 14][..],
                segment.offsets_for(1).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn offsets_for_frame_size_five() {
        let missing_segment = MissingSegment {
            offset: 0,
            length: 10,
        };
        assert_eq!(
            &[0, 5][..],
            missing_segment.offsets_for(5).collect::<Vec<_>>()
        );
    }

    #[test]
    fn offsets_for_frame_size_two() {
        let missing_segment = MissingSegment {
            offset: 0,
            length: 10,
        };
        assert_eq!(
            &[0, 2, 4, 6, 8][..],
            missing_segment.offsets_for(2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fill_in_order() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(0, &[5_u8, 4, 3, 2, 1]).expect("write fail");
        let bytes = buffer.into_inner();
        assert_eq!(&[5_u8, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fill_in_order_produces_no_missing_segments() {
        let mut buffer = BytesQuilt::with_capacity(20);
        for offset in 0..20 {
            buffer.put_at(offset, &[3]).expect("write fail");
        }
        assert!(buffer.missing_segments().next().is_none());
        let bytes = buffer.into_inner();
        assert_eq!(vec![3; 20], bytes.as_ref())
    }

    #[test]
    fn detect_missing_segments() {
        let mut buffer = BytesQuilt::with_capacity(20);
        let missing_segment = buffer.put_at(5, &[5, 4, 3, 2, 1]).expect("write fail");
        assert_eq!(
            Some(MissingSegment {
                offset: 0,
                length: 5
            }),
            missing_segment
        );
    }

    #[test]
    fn detect_multiple_missing_segments() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(5, &[5, 4, 3, 2, 1]).expect("write fail");
        buffer.put_at(15, &[1, 2, 3, 4, 5]).expect("write fail");
        assert_eq!(
            vec![
                MissingSegment {
                    offset: 0,
                    length: 5
                },
                MissingSegment {
                    offset: 10,
                    length: 5
                }
            ],
            buffer.missing_segments().collect::<Vec<_>>()
        );
    }

    #[test]
    fn detect_missing_segments_of_different_sizes() {
        let mut buffer = BytesQuilt::with_capacity(40);
        buffer.put_at(5, &[5, 4, 3, 2, 1]).expect("write fail");
        buffer.put_at(15, &[1, 2, 3, 4, 5]).expect("write fail");
        buffer.put_at(35, &[1, 2, 3, 4, 5]).expect("write fail");
        assert_eq!(
            vec![
                MissingSegment {
                    offset: 0,
                    length: 5
                },
                MissingSegment {
                    offset: 10,
                    length: 5
                },
                MissingSegment {
                    offset: 20,
                    length: 15
                }
            ],
            buffer.missing_segments().collect::<Vec<_>>()
        );
    }

    #[test]
    fn split_missing_segments_on_incomplete_writes() {
        let mut buffer = BytesQuilt::with_capacity(40);
        buffer.put_at(15, &[1, 2, 3, 4, 5]).expect("write fail");
        assert_eq!(
            vec![MissingSegment {
                offset: 0,
                length: 15
            }],
            buffer.missing_segments().collect::<Vec<_>>()
        );
        buffer.put_at(5, &[5, 4, 3, 2, 1]).expect("write fail");
        assert_eq!(
            vec![
                MissingSegment {
                    offset: 0,
                    length: 5
                },
                MissingSegment {
                    offset: 10,
                    length: 5
                },
            ],
            buffer.missing_segments().collect::<Vec<_>>()
        );
    }

    #[test]
    fn fill_out_of_order_start_aligned_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(5, &[5, 4, 3, 2, 1]).expect("write fail");
        buffer.put_at(0, &[10, 9, 8, 7, 6]).expect("write fail");
        let bytes = buffer.into_inner();
        assert_eq!(&[10, 9, 8, 7, 6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn partial_fill_out_of_order_start_aligned_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(4, &[2, 1]).expect("write fail");
        buffer.put_at(0, &[6, 5]).expect("write fail");
        buffer.put_at(2, &[4, 3]).expect("write fail");
        let bytes = buffer.into_inner();
        assert_eq!(&[6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fill_out_of_order_non_aligned_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(4, &[2, 1]).expect("write fail");
        buffer.put_at(2, &[4, 3]).expect("write fail");
        buffer.put_at(0, &[6, 5]).expect("write fail");
        let bytes = buffer.into_inner();
        assert_eq!(&[6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn partial_fill_out_of_order_non_aligned_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(6, &[2, 1]).expect("write fail");
        buffer.put_at(2, &[6, 5]).expect("write fail");
        buffer.put_at(0, &[8, 7]).expect("write fail");
        buffer.put_at(4, &[4, 3]).expect("write fail");
        let bytes = buffer.into_inner();
        assert_eq!(&[8, 7, 6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fails_to_overfill_a_missing_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(4, &[2, 1]).expect("write fail");
        assert_eq!(Err(Error::NotEnoughSpace), buffer.put_at(2, &[4, 3, 7, 8]));
    }

    #[test]
    fn fails_to_overwrite_a_received_segment() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(4, &[2, 1]).expect("write fail");
        buffer.put_at(2, &[4, 3]).expect("write fail");
        assert_eq!(Err(Error::WouldOverwrite), buffer.put_at(2, &[7, 8]));
    }

    #[test]
    fn fails_to_overwrite_a_received_segment_in_the_tail() {
        let mut buffer = BytesQuilt::with_capacity(20);
        buffer.put_at(4, &[2, 1]).expect("write fail");
        assert_eq!(Err(Error::WouldOverwrite), buffer.put_at(4, &[7, 8]));
    }
}
