//! This snippet is from a codebase where I needed to do this operation in many
//! locations and on both usize and u64.
//!
//! Uses the `num-traits` crate.
use num_traits::PrimInt;

pub trait RoundToMultiple: Sized {
    /// Rounds a number up to the nearest multiple of `factor`. This is commonly
    /// used in this crate to ensure file allocations are aligned to page
    /// boundaries.
    fn round_to_multiple_of(self, factor: Self) -> Option<Self>;
}

impl<T> RoundToMultiple for T
where
    T: PrimInt,
{
    fn round_to_multiple_of(self, factor: Self) -> Option<Self> {
        let one = T::one();
        if factor == T::zero() {
            None
        } else if factor == one {
            Some(self)
        } else {
            self.checked_add(&(factor - one))
                .map(|adjusted| adjusted / factor * factor)
        }
    }
}

#[test]
fn round_to_multiple_tests() {
    assert_eq!(5.round_to_multiple_of(0), None);
    assert_eq!(5.round_to_multiple_of(1), Some(5));
    assert_eq!(5.round_to_multiple_of(2), Some(6));
    assert_eq!(5.round_to_multiple_of(3), Some(6));
    assert_eq!(5.round_to_multiple_of(4), Some(8));
    assert_eq!(5.round_to_multiple_of(5), Some(5));

    // Test overflow behavior
    assert_eq!(255_u8.round_to_multiple_of(2), None);

    // Expected use case tests
    assert_eq!(1.round_to_multiple_of(4096), Some(4096));
    assert_eq!(4097.round_to_multiple_of(4096), Some(8192));
}

#[macro_export]
macro_rules! todo_if {
    ($exp:expr) => {
        if $exp {
            todo!()
        }
    };
    ($exp:expr, $message:literal) => {
        if $exp {
            todo!($message)
        }
    };
    ($exp:expr, $message:literal, $args:tt+) => {
        if $exp {
            todo!($message, $(args)+)
        }
    };
}
