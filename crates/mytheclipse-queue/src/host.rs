//! Host-derived sizing for defaults that depend on the machine.
//!
//! Only values that scale with the CPU count live here. Policy knobs (retry
//! counts, timeouts, batch sizes) stay as literals in their `Default` impls:
//! they encode a choice about the work, not about the hardware.

use std::num::NonZeroUsize;

/// Logical CPUs available to this process, falling back to 1 when the platform
/// refuses to report a count.
pub(crate) fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_host() {
        assert_eq!(
            available_parallelism(),
            std::thread::available_parallelism().unwrap().get()
        );
        assert!(available_parallelism() >= 1);
    }
}
