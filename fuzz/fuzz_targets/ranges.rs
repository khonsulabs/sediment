#![no_main]
use libfuzzer_sys::fuzz_target;
use sediment::fuzz_util::{self, SetRange};

fuzz_target!(|sets: Vec<SetRange>| { fuzz_util::test_sets(sets) });
