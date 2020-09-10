use std::mem;

use bytes::{BufMut, Bytes, BytesMut};

use thiserror::Error;

#[derive(Copy, Clone, Debug, Error, PartialEq)]
pub enum Error {
    #[error("Not enough space in buffer segment")]
    NotEnoughSpace,

    #[error("Would overwrite previously received data")]
    WouldOverwrite,
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

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct MissingSegment {
    offset: usize,
    length: usize,
}

impl MissingSegment {
    pub fn offsets_for(self, frame_size: usize) -> impl Iterator<Item = usize> {
        let offset = self.offset;
        let number_of_frames = self.length / frame_size;
        (0..number_of_frames).map(move |index| (index * frame_size) + offset)
    }
}

#[cfg(test)]
mod missing_segment_tests {
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

#[derive(Debug)]
pub struct OutOfOrderBytes {
    tail_offset: usize,
    segments: Vec<Segment>,
    buffer_tail: BytesMut,
}

impl OutOfOrderBytes {
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

    pub fn insert_at_offset(
        &mut self,
        offset: usize,
        bytes: &[u8],
    ) -> Result<Option<MissingSegment>, Error> {
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
                    self.write_offset_at_index(index, offset, bytes)?;
                }
                Err(index) => {
                    // This indexing might be safe because the first
                    // entry in the segments vec should always start
                    // with `offset = 0`
                    let segment = &mut self.segments[index - 1];
                    let to_write_buffer = segment.buffer.split_off(offset - segment.offset);
                    let segment = Segment::missing(offset, to_write_buffer);
                    self.segments.insert(index, segment);
                    self.write_offset_at_index(index, offset, bytes)?;
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
        self.buffer_tail.put(bytes);
        Ok(missing_segment)
    }

    fn missing_segments(&self) -> impl '_ + Iterator<Item = MissingSegment> {
        self.segments.iter().filter_map(Segment::missing_segment)
    }

    pub fn into_bytes(self) -> Bytes {
        let mut segments = self.segments.into_iter();
        if let Some(segment) = segments.next() {
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
            return buffer.freeze();
        }
        self.buffer_tail.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer
            .insert_at_offset(0, &vec![5_u8, 4, 3, 2, 1])
            .expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[5_u8, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fill_in_order_produces_no_missing_segments() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        for offset in 0..20 {
            buffer
                .insert_at_offset(offset, &vec![3])
                .expect("write fail");
        }
        assert!(buffer.missing_segments().collect::<Vec<_>>().is_empty());
        let bytes = buffer.into_bytes();
        assert_eq!(vec![3; 20], bytes.as_ref())
    }

    #[test]
    fn detect_missing_segments() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        let missing_segment = buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
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
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
        buffer
            .insert_at_offset(15, &vec![1, 2, 3, 4, 5])
            .expect("write fail");
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
        let mut buffer = OutOfOrderBytes::with_capacity(40);
        buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
        buffer
            .insert_at_offset(15, &vec![1, 2, 3, 4, 5])
            .expect("write fail");
        buffer
            .insert_at_offset(35, &vec![1, 2, 3, 4, 5])
            .expect("write fail");
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
        let mut buffer = OutOfOrderBytes::with_capacity(40);
        buffer
            .insert_at_offset(15, &vec![1, 2, 3, 4, 5])
            .expect("write fail");
        assert_eq!(
            vec![MissingSegment {
                offset: 0,
                length: 15
            }],
            buffer.missing_segments().collect::<Vec<_>>()
        );
        buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
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
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
        buffer
            .insert_at_offset(0, &vec![10, 9, 8, 7, 6])
            .expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[10, 9, 8, 7, 6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn partial_fill_out_of_order_start_aligned_segment() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(4, &vec![2, 1]).expect("write fail");
        buffer.insert_at_offset(0, &vec![6, 5]).expect("write fail");
        buffer.insert_at_offset(2, &vec![4, 3]).expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fill_out_of_order_non_aligned_segment() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(4, &vec![2, 1]).expect("write fail");
        buffer.insert_at_offset(2, &vec![4, 3]).expect("write fail");
        buffer.insert_at_offset(0, &vec![6, 5]).expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn partial_fill_out_of_order_non_aligned_segment() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(6, &vec![2, 1]).expect("write fail");
        buffer.insert_at_offset(2, &vec![6, 5]).expect("write fail");
        buffer.insert_at_offset(0, &vec![8, 7]).expect("write fail");
        buffer.insert_at_offset(4, &vec![4, 3]).expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[8, 7, 6, 5, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn fails_to_overfill_a_missing_segment() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(4, &vec![2, 1]).expect("write fail");
        assert_eq!(
            Err(Error::NotEnoughSpace),
            buffer.insert_at_offset(2, &vec![4, 3, 7, 8])
        );
    }

    #[test]
    fn fails_to_overwrite_a_received_segment() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(4, &vec![2, 1]).expect("write fail");
        buffer.insert_at_offset(2, &vec![4, 3]).expect("write fail");
        assert_eq!(
            Err(Error::WouldOverwrite),
            buffer.insert_at_offset(2, &vec![7, 8])
        );
    }

    #[test]
    fn fails_to_overwrite_a_received_segment_in_the_tail() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer.insert_at_offset(4, &vec![2, 1]).expect("write fail");
        assert_eq!(
            Err(Error::WouldOverwrite),
            buffer.insert_at_offset(4, &vec![7, 8])
        );
    }
}
