use crate::{Error, Result};

pub fn usize_to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(Error::from)
}

pub fn u32_to_usize(value: u32) -> Result<usize> {
    usize::try_from(value).map_err(Error::from)
}
