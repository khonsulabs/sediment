use std::ops::{Deref, DerefMut, Range, RangeBounds};

#[derive(Debug, Default)]
pub struct IoBuffer {
    pub buffer: Vec<u8>,
    pub range: Option<Range<usize>>,
}

impl IoBuffer {
    pub fn advance_by(&mut self, bytes: usize) {
        self.range = match &self.range {
            Some(range) => {
                let start = range.start.saturating_add(bytes).min(range.end);
                Some(start..range.end)
            }
            None => {
                let end = self.buffer.len();
                let start = bytes.min(end);
                Some(start..end)
            }
        };
    }
}

impl From<Vec<u8>> for IoBuffer {
    fn from(buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            range: None,
        }
    }
}

impl Deref for IoBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.range {
            Some(range) => &self.buffer[range.clone()],
            None => &self.buffer,
        }
    }
}

impl DerefMut for IoBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &self.range {
            Some(range) => &mut self.buffer[range.clone()],
            None => &mut self.buffer,
        }
    }
}

pub trait IoBufferExt: Sized {
    fn io(self) -> IoBuffer {
        self.io_slice(..)
    }

    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer;
}

impl IoBufferExt for Vec<u8> {
    #[allow(clippy::range_plus_one)] // a type requirement for the underlying structure
    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer {
        IoBuffer {
            range: match (range.start_bound(), range.end_bound()) {
                (std::ops::Bound::Included(start), std::ops::Bound::Included(end)) => {
                    Some(*start..*end + 1)
                }
                (std::ops::Bound::Included(start), std::ops::Bound::Excluded(end)) => {
                    Some(*start..*end)
                }
                (std::ops::Bound::Included(start), std::ops::Bound::Unbounded) => {
                    Some(*start..self.len())
                }
                (std::ops::Bound::Excluded(start), std::ops::Bound::Included(end)) => {
                    Some(*start + 1..*end + 1)
                }
                (std::ops::Bound::Excluded(start), std::ops::Bound::Excluded(end)) => {
                    Some(*start + 1..*end)
                }
                (std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded) => {
                    Some(*start + 1..self.len())
                }
                (std::ops::Bound::Unbounded, std::ops::Bound::Included(end)) => Some(0..*end + 1),
                (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(end)) => Some(0..*end),
                (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => None,
            },
            buffer: self,
        }
    }
}
