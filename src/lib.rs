#![cfg_attr(feature = "libc", deny(unsafe_code))]
#![cfg_attr(not(feature = "libc"), forbid(unsafe_code))]

pub mod database;
pub mod format;
pub mod io;
pub mod ranges;

#[cfg(feature = "test-util")]
pub mod fuzz_util;
