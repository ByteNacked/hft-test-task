use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;
use triple_buffer::{Input, Output, TripleBuffer};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FeedState {
    #[default]
    Connecting,
    Buffering,
    SnapshotLoading,
    Replaying,
    Live,
    Backoff,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedFault {
    Http,
    Gap,
    Disconnect,
    Decode,
    Overflow,
    Coverage,
    Timeout,
    Numeric,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FeedMetrics {
    pub state: FeedState,
    pub connection_epoch: u64,
    pub sync_epoch: u64,
    pub last_fault: Option<FeedFault>,
    pub frames: u64,
    pub applied_diffs: u64,
    pub snapshots: u64,
    pub stale_snapshots: u64,
    pub gaps: u64,
    pub resyncs: u64,
    pub overflows: u64,
    pub decode_errors: u64,
    pub coverage_lost: u64,
    pub duplicates: u64,
    pub reconnects: u64,
    pub last_received_at: Option<std::time::Instant>,
}
pub fn channel() -> (Input<FeedMetrics>, Output<FeedMetrics>) {
    TripleBuffer::default().split()
}

pub fn init_logging() -> WorkerGuard {
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(crate::config::LOG_RECORDS)
        .lossy(true)
        .finish(std::io::stderr());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .json()
        .init();
    guard
}
