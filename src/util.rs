use std::io;

use crate::Result;

pub fn usize_to_u32(value: usize) -> Result<u32> {
    match u32::try_from(value) {
        Ok(value) => Ok(value),
        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err)),
    }
}

pub fn u32_to_usize(value: u32) -> Result<usize> {
    match usize::try_from(value) {
        Ok(value) => Ok(value),
        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err)),
    }
}
