use std::{io::ErrorKind, num::TryFromIntError};

use crate::io;

pub trait ToIoResult<T> {
    fn to_io(self) -> io::Result<T>;
}

impl<T> ToIoResult<T> for Result<T, TryFromIntError> {
    fn to_io(self) -> io::Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(err) => Err(std::io::Error::new(ErrorKind::Other, err)),
        }
    }
}
