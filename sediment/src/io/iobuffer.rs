use std::ops::{Deref, DerefMut, Range, RangeBounds};

use rebytes::Buffer;

#[derive(Debug, Default)]
pub struct IoBuffer<B> {
    pub buffer: B,
    pub range: Option<Range<usize>>,
}

impl<B> IoBuffer<B>
where
    B: Backing,
{
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

impl From<Vec<u8>> for IoBuffer<Vec<u8>> {
    fn from(buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            range: None,
        }
    }
}

impl From<Buffer> for IoBuffer<Buffer> {
    fn from(buffer: Buffer) -> Self {
        Self {
            buffer,
            range: None,
        }
    }
}

impl From<AnyBacking> for IoBuffer<AnyBacking> {
    fn from(buffer: AnyBacking) -> Self {
        Self {
            buffer,
            range: None,
        }
    }
}

impl<B> IoBuffer<B>
where
    B: Into<AnyBacking>,
{
    pub fn map_any(self) -> IoBuffer<AnyBacking> {
        IoBuffer {
            buffer: self.buffer.into(),
            range: self.range,
        }
    }
}

impl<B> Deref for IoBuffer<B>
where
    B: Backing,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.range {
            Some(range) => &self.buffer[range.clone()],
            None => &self.buffer,
        }
    }
}

impl<B> DerefMut for IoBuffer<B>
where
    B: Backing,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &self.range {
            Some(range) => &mut self.buffer[range.clone()],
            None => &mut self.buffer,
        }
    }
}

pub trait Backing: Deref<Target = [u8]> + DerefMut + Sized {
    fn capacity(&self) -> usize;
    fn io(self) -> IoBuffer<Self> {
        self.io_slice(..)
    }

    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer<Self>;
}

impl Backing for Vec<u8> {
    #[allow(clippy::range_plus_one)] // a type requirement for the underlying structure
    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer<Self> {
        IoBuffer {
            range: simplify_range(range, self.len()),
            buffer: self,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl Backing for Buffer {
    fn capacity(&self) -> usize {
        self.capacity()
    }
    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer<Self> {
        IoBuffer {
            range: simplify_range(range, self.len()),
            buffer: self,
        }
    }
}

impl Backing for AnyBacking {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    #[allow(clippy::range_plus_one)] // a type requirement for the underlying structure
    fn io_slice(self, range: impl RangeBounds<usize>) -> IoBuffer<Self> {
        IoBuffer {
            range: simplify_range(range, self.len()),
            buffer: self,
        }
    }
}

#[allow(clippy::range_plus_one)] // a type requirement for the underlying structure
fn simplify_range(range: impl RangeBounds<usize>, length: usize) -> Option<Range<usize>> {
    match (range.start_bound(), range.end_bound()) {
        (std::ops::Bound::Included(start), std::ops::Bound::Included(end)) => {
            Some(*start..*end + 1)
        }
        (std::ops::Bound::Included(start), std::ops::Bound::Excluded(end)) => Some(*start..*end),
        (std::ops::Bound::Included(start), std::ops::Bound::Unbounded) => Some(*start..length),
        (std::ops::Bound::Excluded(start), std::ops::Bound::Included(end)) => {
            Some(*start + 1..*end + 1)
        }
        (std::ops::Bound::Excluded(start), std::ops::Bound::Excluded(end)) => {
            Some(*start + 1..*end)
        }
        (std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded) => Some(*start + 1..length),
        (std::ops::Bound::Unbounded, std::ops::Bound::Included(end)) => Some(0..*end + 1),
        (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(end)) => Some(0..*end),
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => None,
    }
}

#[derive(Debug)]
pub enum AnyBacking {
    Vec(Vec<u8>),
    Buffer(Buffer),
}

impl Default for AnyBacking {
    fn default() -> Self {
        AnyBacking::Vec(Vec::default())
    }
}

impl AnyBacking {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            AnyBacking::Vec(vec) => vec.len(),
            AnyBacking::Buffer(buf) => buf.len(),
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        match self {
            AnyBacking::Vec(vec) => vec.capacity(),
            AnyBacking::Buffer(buf) => buf.capacity(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for AnyBacking {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            AnyBacking::Vec(vec) => &*vec,
            AnyBacking::Buffer(buf) => &*buf,
        }
    }
}

impl DerefMut for AnyBacking {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            AnyBacking::Vec(vec) => &mut *vec,
            AnyBacking::Buffer(buf) => &mut *buf,
        }
    }
}

impl From<Vec<u8>> for AnyBacking {
    fn from(buf: Vec<u8>) -> Self {
        Self::Vec(buf)
    }
}

impl From<Buffer> for AnyBacking {
    fn from(buf: Buffer) -> Self {
        Self::Buffer(buf)
    }
}

impl From<AnyBacking> for Vec<u8> {
    fn from(value: AnyBacking) -> Self {
        match value {
            AnyBacking::Vec(vec) => vec,
            AnyBacking::Buffer(_) => unreachable!("invalid conversion"),
        }
    }
}

impl From<AnyBacking> for Buffer {
    fn from(value: AnyBacking) -> Self {
        match value {
            AnyBacking::Buffer(buf) => buf,
            AnyBacking::Vec(_) => unreachable!("invalid conversion"),
        }
    }
}
