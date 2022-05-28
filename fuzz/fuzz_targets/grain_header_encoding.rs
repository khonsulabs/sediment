#![no_main]
use libfuzzer_sys::fuzz_target;
use sediment::fuzz_util::{self, AllocationAmount};

fuzz_target!(|allocations: Vec<AllocationAmount>| {
    fuzz_util::test_grain_header_encoding(allocations)
});
