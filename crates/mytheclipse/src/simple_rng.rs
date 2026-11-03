//! Minimal PRNG so the crate does not need the `rand` dependency.
//!
//! Jitter is the only consumer: it needs a cheap, thread-local-ish source of
//! uniform values, not cryptographic quality. `xoshiro256**` (Blackman & Vigna,
//! public domain) is used because it is a handful of ALU ops per draw and has a
//! far longer period than the retry loop can ever exhaust.

/// Uniform random source.
pub trait Rng {
    /// Next 64-bit draw.
    fn next_u64(&mut self) -> u64;

    /// Uniform `f64` in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        // 53 bits of mantissa is the most an f64 can represent exactly.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// `xoshiro256**` generator state.
#[derive(Debug, Clone)]
pub struct Xoshiro256 {
    state: [u64; 4],
}

impl Xoshiro256 {
    /// Seeds the generator. `seed` is spread over the four words by
    /// [`SplitMix64`](https://prng.di.unimi.it/splitmix64.c) so nearby seeds
    /// (a nanosecond clock, say) still produce unrelated streams.
    pub fn new(seed: u64) -> Self {
        let mut z = seed;
        let mut state = [0u64; 4];
        for word in &mut state {
            z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut x = z;
            x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            *word = x ^ (x >> 31);
        }
        Self { state }
    }
}

impl Rng for Xoshiro256 {
    fn next_u64(&mut self) -> u64 {
        let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.state[1] << 17;

        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);

        result
    }
}

/// A generator seeded from the clock and the process id, so two processes
/// starting in the same nanosecond do not jitter in lockstep.
pub fn random_rng() -> Xoshiro256 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    Xoshiro256::new(nanos ^ ((std::process::id() as u64) << 32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Xoshiro256::new(7);
        let mut b = Xoshiro256::new(7);
        for _ in 0..8 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Xoshiro256::new(7);
        let mut b = Xoshiro256::new(8);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn consecutive_draws_differ() {
        let mut rng = Xoshiro256::new(1);
        let first = rng.next_u64();
        assert_ne!(first, rng.next_u64());
    }

    #[test]
    fn f64_is_in_unit_interval() {
        let mut rng = Xoshiro256::new(3);
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v), "{v} outside [0, 1)");
        }
    }
}
