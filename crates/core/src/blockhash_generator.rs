use rand::{rngs::ThreadRng, Rng};
use solana_hash::Hash;

pub struct DummyRpcBlockhashGenerator {
    rng: ThreadRng,
}

impl DummyRpcBlockhashGenerator {
    pub fn new() -> Self {
        Self {
            rng: rand::thread_rng(),
        }
    }

    pub fn next(&mut self) -> Hash {
        let mut hash_buffer = [0u8; 32];
        self.rng.fill(&mut hash_buffer);
        Hash::new(&hash_buffer)
    }
}
