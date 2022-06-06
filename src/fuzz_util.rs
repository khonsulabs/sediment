use arbitrary::{Arbitrary, Unstructured};
use std::collections::BTreeMap;

use crate::ranges::Ranges;

#[derive(Debug)]
pub struct SetRange {
    start: u8,
    end: u8,
    value: u8,
}

impl<'a> Arbitrary<'a> for SetRange {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let start = u.arbitrary::<u8>()?;
        let end = u.arbitrary::<u8>()?.max(start);
        let value = u.arbitrary::<u8>()?;
        Ok(SetRange { start, end, value })
    }
}

fn extend_u8(value: u8, min: bool) -> u64 {
    let value = u64::from(value) << 56;
    if min {
        value
    } else {
        value | 0xFF_FFFF_FFFF_FFFF
    }
}

pub fn test_sets(sets: Vec<SetRange>) {
    let mut oracle = BTreeMap::new();
    let mut ranges = Ranges::default();
    for op in sets {
        // expand to take up the entire u64 space
        let start = extend_u8(op.start, true);
        let end = extend_u8(op.end, false);

        for i in op.start..=op.end {
            oracle.insert(i, op.value);
        }

        match (op.start == 0, op.end == u8::MAX) {
            (true, true) => {
                ranges.set(.., op.value);
            }
            (true, false) => {
                ranges.set(..=end, op.value);
            }
            (false, true) => {
                ranges.set(start.., op.value);
            }
            (false, false) => {
                ranges.set(start..=end, op.value);
            }
        };
    }

    for (key, value) in &oracle {
        let min = ranges.get(extend_u8(*key, false));
        let max = ranges.get(extend_u8(*key, true));
        if min != value || max != value {
            panic!("Error: {key} returned {min}/{max} instead of {value}. Oracle: {oracle:#?}; Ranges: {ranges:#?}")
        }
    }
}

#[test]
#[cfg(feature = "test-util")]
fn test_remove_start() {
    test_sets(vec![
        SetRange {
            start: 0,
            end: 0,
            value: 255,
        },
        SetRange {
            start: 1,
            end: 1,
            value: 255,
        },
        SetRange {
            start: 0,
            end: 0,
            value: 0,
        },
    ]);
}

#[test]
#[cfg(feature = "test-util")]
fn test_clear_from_start_partial() {
    test_sets(vec![
        SetRange {
            start: 66,
            end: 255,
            value: 115,
        },
        SetRange {
            start: 0,
            end: 250,
            value: 0,
        },
    ]);
}

#[test]
#[cfg(feature = "test-util")]
fn test_clear_partial_from_start() {
    test_sets(vec![
        SetRange {
            start: 66,
            end: 255,
            value: 250,
        },
        SetRange {
            start: 255,
            end: 255,
            value: 0,
        },
        SetRange {
            start: 0,
            end: 250,
            value: 0,
        },
    ]);
}

#[test]
#[cfg(feature = "test-util")]
fn test_remove_middle_touching_last() {
    test_sets(vec![
        SetRange {
            start: 66,
            end: 255,
            value: 250,
        },
        SetRange {
            start: 250,
            end: 255,
            value: 255,
        },
        SetRange {
            start: 115,
            end: 250,
            value: 0,
        },
    ])
}

#[test]
#[cfg(feature = "test-util")]
fn test_remove_from_start_exact_end() {
    test_sets(vec![
        SetRange {
            start: 26,
            end: 255,
            value: 255,
        },
        SetRange {
            start: 64,
            end: 64,
            value: 0,
        },
        SetRange {
            start: 0,
            end: 64,
            value: 231,
        },
    ])
}
