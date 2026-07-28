use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentState {
    pub agent_id: String,
    pub model: String,
    pub status: String,
    pub task: String,
    pub progress: u8,
    pub tokens: u64,
    pub memory_mb: u64,
    pub cpu_percent: f32,
    pub files_processed: u64,
    pub current_file: String,
}

impl AgentState {
    pub fn demo() -> Self {
        Self {
            agent_id: "agent-17".into(),
            model: "qwen3-coder".into(),
            status: "running".into(),
            task: "analyze repository".into(),
            progress: 42,
            tokens: 18_492,
            memory_mb: 4_812,
            cpu_percent: 34.2,
            files_processed: 173,
            current_file: "src/engine/parser.rs".into(),
        }
    }

    pub fn advance(&self) -> Self {
        let mut next = self.clone();
        next.progress = next.progress.saturating_add(1).min(100);
        next.tokens = next.tokens.saturating_add(128);
        next.cpu_percent = (next.cpu_percent + 1.9) % 100.0;
        next.files_processed = next.files_processed.saturating_add(1);
        next
    }
}
