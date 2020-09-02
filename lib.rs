use std::mem;

use bytes::{BufMut, Bytes, BytesMut};

#[derive(Copy, Clone, Debug, PartialEq)]
enum Error {
    NotEnoughSpace,
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
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct MissingSegment {
    offset: usize,
    length: usize,
}

struct OutOfOrderBytes {
    tail_offset: usize,
    segments: Vec<Segment>,
    buffer_tail: BytesMut,
}

impl OutOfOrderBytes {
    fn with_capacity(capacity: usize) -> Self {
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

    fn insert_at_offset(&mut self, offset: usize, bytes: &[u8]) -> Result<(), Error> {
        debug_assert!(self
            .segments
            .first()
            .map(|segment| segment.offset == 0)
            .unwrap_or(true));
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
            return Ok(());
        } else if self.tail_offset != offset {
            if !self.buffer_tail.is_empty() {
                let head_received_bytes = self.buffer_tail.split();
                self.tail_offset += head_received_bytes.len();
                self.segments
                    .push(Segment::received(self.tail_offset, head_received_bytes));
            }

            let head_offset = self.tail_offset;
            self.tail_offset = offset;

            let tail_bytes = self.buffer_tail.split_off(offset - head_offset);
            let head_bytes = mem::replace(&mut self.buffer_tail, tail_bytes);

            // This is true because of the conditional split above to
            // identify and store a received segment
            debug_assert!(head_bytes.is_empty());
            self.segments
                .push(Segment::missing(head_offset, head_bytes));
        } else if !self.buffer_tail.is_empty() {
            return Err(Error::WouldOverwrite);
        }
        self.buffer_tail.put(bytes);
        Ok(())
    }

    fn missing_segments(&self) -> impl '_ + Iterator<Item = MissingSegment> {
        self.segments
            .iter()
            .filter_map(|segment| match segment.status {
                Status::Missing => Some(MissingSegment {
                    offset: segment.offset,
                    length: segment.buffer.capacity(),
                }),
                Status::Received => None,
            })
    }

    fn into_bytes(self) -> Bytes {
        let mut segments = self.segments.into_iter();
        if let Some(first_segment) = segments.next() {
            debug_assert!(first_segment.status == Status::Received);
            let mut buffer: BytesMut = first_segment.buffer;
            for segment in segments {
                debug_assert!(first_segment.status == Status::Received);
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
    fn fill_in_order() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer
            .insert_at_offset(0, &vec![5_u8, 4, 3, 2, 1])
            .expect("write fail");
        let bytes = buffer.into_bytes();
        assert_eq!(&[5_u8, 4, 3, 2, 1][..], bytes.as_ref())
    }

    #[test]
    fn detect_missing_segments() {
        let mut buffer = OutOfOrderBytes::with_capacity(20);
        buffer
            .insert_at_offset(5, &vec![5, 4, 3, 2, 1])
            .expect("write fail");
        assert_eq!(
            Some(MissingSegment {
                offset: 0,
                length: 5
            }),
            buffer.missing_segments().next()
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
