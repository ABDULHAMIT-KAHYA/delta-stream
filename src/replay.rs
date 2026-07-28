use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct ReplayWindow {
    capacity: usize,
    seen: VecDeque<u64>,
}

impl ReplayWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: VecDeque::with_capacity(capacity.max(1)),
        }
    }
    pub fn contains(&self, sequence: u64) -> bool {
        self.seen.contains(&sequence)
    }
    pub fn clear(&mut self) {
        self.seen.clear();
    }
    pub fn record(&mut self, sequence: u64) {
        if self.contains(sequence) {
            return;
        }
        self.seen.push_back(sequence);
        while self.seen.len() > self.capacity {
            self.seen.pop_front();
        }
    }
}

impl Default for ReplayWindow {
    fn default() -> Self {
        Self::new(256)
    }
}
