use std::collections::BTreeMap;

/// Tracks out-of-order completions for contiguous ranges and exposes the next
/// unfinished slot (the "frontier").
#[derive(Debug, Default)]
pub struct InOrderRetirement {
    frontier: u64,
    pending: BTreeMap<u64, u64>,
}

impl InOrderRetirement {
    /// Create a new retirement tracker starting at the provided frontier.
    pub const fn new(frontier: u64) -> Self {
        Self {
            frontier,
            pending: BTreeMap::new(),
        }
    }

    /// Current frontier (the next slot that must be retired).
    pub const fn frontier(&self) -> u64 {
        self.frontier
    }

    /// Synchronise the tracker with an externally persisted frontier.
    /// Returns `Some(new_frontier)` when the internal frontier advanced.
    pub fn sync_frontier(&mut self, frontier: u64) -> Option<u64> {
        if frontier > self.frontier {
            self.frontier = frontier;
            self.pending.retain(|start, _| *start >= self.frontier);
            self.advance_frontier().then_some(self.frontier)
        } else {
            None
        }
    }

    /// Record a completed [start, end] range. When this fills any gap starting
    /// from the current frontier, the frontier advances and the updated value
    /// is returned.
    pub fn record_range(&mut self, start: u64, end: u64) -> Option<u64> {
        if start > end {
            return None;
        }

        let effective_start = start.max(self.frontier);
        if effective_start > end {
            return None;
        }

        self.pending
            .entry(effective_start)
            .and_modify(|existing_end| {
                if end > *existing_end {
                    *existing_end = end;
                }
            })
            .or_insert(end);

        self.advance_frontier().then_some(self.frontier)
    }

    fn advance_frontier(&mut self) -> bool {
        let mut advanced = false;
        while let Some(range_end) = self.pending.remove(&self.frontier) {
            self.frontier = range_end.saturating_add(1);
            advanced = true;
        }
        if advanced {
            let cutoff = self.frontier;
            self.pending.retain(|start, _| *start >= cutoff);
        }
        advanced
    }
}
