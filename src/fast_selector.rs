use std::collections::VecDeque;

use crate::smart_delta::SmartDeltaKind;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChangeProfile {
    pub previous_len: usize,
    pub current_len: usize,
    pub changed_bytes: usize,
    pub changed_runs: usize,
    pub common_prefix: usize,
    pub common_suffix: usize,
}

impl ChangeProfile {
    pub fn analyze(previous: &[u8], current: &[u8]) -> Self {
        let common = previous.len().min(current.len());
        let mut changed_bytes = previous.len().abs_diff(current.len());
        let mut changed_runs = 0usize;
        let mut in_run = false;
        for i in 0..common {
            let changed = previous[i] != current[i];
            if changed {
                changed_bytes += 1;
                if !in_run {
                    changed_runs += 1;
                }
            }
            in_run = changed;
        }
        let mut prefix = 0usize;
        while prefix < common && previous[prefix] == current[prefix] {
            prefix += 1;
        }
        let mut suffix = 0usize;
        let suffix_limit = common.saturating_sub(prefix);
        while suffix < suffix_limit
            && previous[previous.len() - 1 - suffix] == current[current.len() - 1 - suffix]
        {
            suffix += 1;
        }
        Self {
            previous_len: previous.len(),
            current_len: current.len(),
            changed_bytes,
            changed_runs,
            common_prefix: prefix,
            common_suffix: suffix,
        }
    }

    pub fn change_ratio(&self) -> f64 {
        let denom = self.previous_len.max(self.current_len).max(1);
        self.changed_bytes as f64 / denom as f64
    }

    pub fn resized(&self) -> bool {
        self.previous_len != self.current_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectorPolicy {
    pub sparse_ratio: u8,
    pub range_run_limit: usize,
    pub explore_every: u64,
    pub winner_window: usize,
}

impl Default for SelectorPolicy {
    fn default() -> Self {
        Self {
            sparse_ratio: 8,
            range_run_limit: 64,
            explore_every: 64,
            winner_window: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyAdvisor {
    policy: SelectorPolicy,
    recent_winners: VecDeque<SmartDeltaKind>,
    updates: u64,
    compression_attempts: u64,
    compression_wins: u64,
}

impl StrategyAdvisor {
    pub fn new(policy: SelectorPolicy) -> Self {
        Self {
            policy,
            recent_winners: VecDeque::with_capacity(policy.winner_window.max(1)),
            updates: 0,
            compression_attempts: 0,
            compression_wins: 0,
        }
    }

    pub fn shortlist(&self, profile: ChangeProfile) -> Vec<SmartDeltaKind> {
        if profile.resized() {
            return vec![SmartDeltaKind::Splice, SmartDeltaKind::Chunks];
        }
        if let Some(winner) = self.dominant_winner() {
            if !self.should_explore() {
                return vec![winner];
            }
        }
        let pct = profile.change_ratio() * 100.0;
        if pct <= self.policy.sparse_ratio as f64 {
            vec![SmartDeltaKind::Sparse, SmartDeltaKind::Ranges]
        } else if profile.changed_runs <= self.policy.range_run_limit {
            vec![SmartDeltaKind::Ranges, SmartDeltaKind::Sparse]
        } else if pct >= 45.0 {
            vec![SmartDeltaKind::Xor, SmartDeltaKind::Ranges]
        } else {
            vec![SmartDeltaKind::Ranges, SmartDeltaKind::Sparse]
        }
    }

    pub fn observe_winner(&mut self, kind: SmartDeltaKind) {
        self.updates = self.updates.saturating_add(1);
        if self.recent_winners.len() == self.policy.winner_window.max(1) {
            self.recent_winners.pop_front();
        }
        self.recent_winners.push_back(kind);
    }

    pub fn observe_no_delta(&mut self) {
        self.updates = self.updates.saturating_add(1);
    }

    pub fn observe_compression(&mut self, won: bool) {
        self.compression_attempts = self.compression_attempts.saturating_add(1);
        if won {
            self.compression_wins = self.compression_wins.saturating_add(1);
        }
    }

    pub fn compression_win_rate(&self) -> f64 {
        if self.compression_attempts == 0 {
            0.0
        } else {
            self.compression_wins as f64 / self.compression_attempts as f64
        }
    }

    pub fn should_compress(&self, payload_len: usize, min_payload: usize) -> bool {
        if payload_len < min_payload {
            return false;
        }
        self.compression_attempts < 16
            || self.compression_win_rate() >= 0.15
            || self.should_explore()
    }

    fn dominant_winner(&self) -> Option<SmartDeltaKind> {
        if self.recent_winners.len() < 8 {
            return None;
        }
        let mut counts = [0usize; 5];
        for k in &self.recent_winners {
            counts[index(*k)] += 1;
        }
        let (idx, count) = counts.into_iter().enumerate().max_by_key(|(_, c)| *c)?;
        if count * 4 < self.recent_winners.len() * 3 {
            return None;
        }
        Some(from_index(idx))
    }

    fn should_explore(&self) -> bool {
        self.policy.explore_every > 0
            && self.updates > 0
            && self.updates.is_multiple_of(self.policy.explore_every)
    }
}

impl Default for StrategyAdvisor {
    fn default() -> Self {
        Self::new(SelectorPolicy::default())
    }
}

fn index(k: SmartDeltaKind) -> usize {
    match k {
        SmartDeltaKind::Sparse => 0,
        SmartDeltaKind::Ranges => 1,
        SmartDeltaKind::Xor => 2,
        SmartDeltaKind::Splice => 3,
        SmartDeltaKind::Chunks => 4,
    }
}
fn from_index(i: usize) -> SmartDeltaKind {
    match i {
        0 => SmartDeltaKind::Sparse,
        1 => SmartDeltaKind::Ranges,
        2 => SmartDeltaKind::Xor,
        3 => SmartDeltaKind::Splice,
        _ => SmartDeltaKind::Chunks,
    }
}
