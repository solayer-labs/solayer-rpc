use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use metrics::Histogram;

/// A struct that take a reference to a `Recordable` type and records the time
/// taken
///
/// The record is represented as a duration in milliseconds
pub struct ScopedTimer<'a, T: Recordable> {
    start: Instant,
    inner: &'a T,
}

impl<'a, T: Recordable> ScopedTimer<'a, T> {
    pub fn new(inner: &'a T) -> Self {
        Self {
            start: Instant::now(),
            inner,
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl<T: Recordable> Drop for ScopedTimer<'_, T> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.inner.record(duration.as_millis());
    }
}

pub trait Recordable {
    fn record(&self, duration_ms: u128);
}

impl Recordable for Histogram {
    fn record(&self, duration: u128) {
        self.record(duration as f64);
    }
}

impl Recordable for AtomicU64 {
    fn record(&self, duration: u128) {
        self.store(duration as u64, Ordering::Relaxed);
    }
}
