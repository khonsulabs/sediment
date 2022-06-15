use std::{
    io::{self, ErrorKind},
    path::Path,
};

pub use io::Result;

use crate::io::{
    iobuffer::{IoBuffer, IoBufferExt},
    paths::PathId,
};

pub mod fs;
pub mod iobuffer;
pub mod memory;
pub mod paths;
pub mod uring;

pub mod any;
pub mod ext;
#[cfg(target_os = "linux")]
mod linux;

pub trait FileManager: Default {
    type File: File<Manager = Self>;
    fn resolve_path(&self, path: impl AsRef<Path>) -> PathId;

    fn read(&self, path: &PathId) -> io::Result<Self::File>;
    fn write(&self, path: &PathId) -> io::Result<Self::File>;
}

pub trait File {
    type Manager: FileManager<File = Self>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }

    fn read_at(&mut self, buffer: impl Into<IoBuffer>, position: u64) -> BufferResult<usize>;
    fn read_exact(&mut self, buffer: impl Into<IoBuffer>, mut position: u64) -> BufferResult<()> {
        let buffer = buffer.into();
        let (mut start, end) = buffer.range.clone().map_or_else(
            || (0, buffer.buffer.len()),
            |range| (range.start, range.end),
        );
        let mut buffer = buffer.buffer;
        while start < end {
            buffer = match self.read_at(buffer.io_slice(start..end), position) {
                (Ok(bytes_read), original_buffer) if bytes_read == 0 => {
                    return (
                        Err(std::io::Error::from(ErrorKind::UnexpectedEof)),
                        original_buffer,
                    );
                }
                (Ok(bytes_read), original_buffer) => {
                    start += bytes_read;
                    position += u64::try_from(bytes_read).unwrap();
                    original_buffer
                }
                (Err(other), original_buffer) => return (Err(other), original_buffer),
            };
        }
        (Ok(()), buffer)
    }

    fn write_at(&mut self, buffer: impl Into<IoBuffer>, position: u64) -> BufferResult<usize>;
    fn write_all(&mut self, buffer: impl Into<IoBuffer>, mut position: u64) -> BufferResult<()> {
        let buffer = buffer.into();
        let (mut start, end) = buffer.range.clone().map_or_else(
            || (0, buffer.buffer.len()),
            |range| (range.start, range.end),
        );
        let mut buffer = buffer.buffer;
        while start < end {
            buffer = match self.write_at(buffer.io_slice(start..end), position) {
                (Ok(bytes_written), original_buffer) => {
                    start += bytes_written;
                    position += u64::try_from(bytes_written).unwrap();
                    original_buffer
                }
                (Err(other), original_buffer) => return (Err(other), original_buffer),
            };
        }
        (Ok(()), buffer)
    }

    fn synchronize(&mut self) -> io::Result<()>;
    fn set_length(&mut self, new_length: u64) -> io::Result<()>;
}

pub type BufferResult<T> = (io::Result<T>, Vec<u8>);

#[cfg(test)]
#[macro_export]
macro_rules! io_test {
    ($name:ident, $test_body:block) => {
        #[cfg(test)]
        #[allow(unused_imports)]
        mod $name {
            use std::path::PathBuf;

            use $crate::io::{
                fs::StdFileManager, iobuffer::IoBufferExt, memory::MemoryFileManager, File,
                FileManager,
            };

            use super::*;

            #[allow(dead_code)]
            fn unique_file_path<Manager>() -> PathBuf {
                PathBuf::from(format!(
                    ".test-{}-{}.sediment",
                    stringify!($name),
                    std::any::type_name::<Manager>()
                        .rsplit_once("::")
                        .unwrap()
                        .1
                ))
            }
            #[allow(dead_code)]
            fn name_of<Manager>() -> &'static str {
                std::any::type_name::<Manager>()
                    .rsplit_once("::")
                    .unwrap()
                    .1
            }

            fn test<Manager: FileManager>() {
                $test_body
            }

            #[test]
            fn fs() {
                test::<StdFileManager>();
            }

            #[test]
            fn memory() {
                test::<MemoryFileManager>();
            }
        }
    };
}

#[cfg(test)]
io_test!(basics, {
    let manager = Manager::default();
    let path = manager.resolve_path(unique_file_path::<Manager>());
    let mut file = manager.write(&path).unwrap();
    let data = b"hello, world".to_vec();
    let (result, data) = file.write_at(data, 0);
    let bytes_written = result.unwrap();
    assert_eq!(bytes_written, data.len());
    file.synchronize().unwrap();

    // Test overwriting.
    let data = b"new world".to_vec();
    // Replace "worl" with "new " (Overwrite without extend)
    let (result, data) = file.write_at(data.io_slice(..4), 7);
    let bytes_written = result.unwrap();
    assert_eq!(bytes_written, 4);

    // Replace final "d" with "new world" (Overwrite with extend)
    let (result, data) = file.write_at(data, 11);
    let bytes_written = result.unwrap();
    assert_eq!(bytes_written, data.len());
    drop(file);

    let expected_result = b"hello, new new world";
    let mut file = manager.read(&path).unwrap();
    let buffer = vec![0; expected_result.len()];
    let (result, buffer) = file.read_at(buffer.io_slice(..4), 0);
    let bytes_read = result.unwrap();
    assert_eq!(bytes_read, 4);
    let (result, buffer) = file.read_at(buffer.io_slice(4..expected_result.len()), 4);
    let bytes_read = result.unwrap();
    assert_eq!(bytes_read, expected_result.len() - 4);
    assert_eq!(buffer, expected_result);
    drop(file);

    if path.as_ref().exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

pub(crate) fn invalid_data_error<E>(message: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::new(ErrorKind::InvalidData, message)
}
