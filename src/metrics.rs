use std::io::IsTerminal;
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        FmtContext, FormatEvent, FormatFields,
        format::Writer,
        time::{FormatTime, SystemTime},
    },
    registry::LookupSpan,
};

struct TextFormat {
    color: bool,
}
impl<S, N> FormatEvent<S, N> for TextFormat
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let metadata = event.metadata();
        let background = if !self.color {
            None
        } else if matches!(
            *metadata.level(),
            tracing::Level::WARN | tracing::Level::ERROR
        ) || metadata.name() == "instrument_offline"
        {
            Some("\x1b[41;97m")
        } else if metadata.name() == "instrument_online" {
            Some("\x1b[44;97m")
        } else if metadata.name() == "bias_changed" {
            Some("\x1b[42;30m")
        } else {
            None
        };
        if let Some(color) = background {
            write!(writer, "{color}")?;
        }
        write!(writer, "{}:", metadata.level())?;
        SystemTime.format_time(&mut writer)?;
        write!(writer, ": ")?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        if background.is_some() {
            write!(writer, "\x1b[0m")?;
        }
        writeln!(writer)
    }
}
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
    /// Last successfully applied exchange update ID (`u`), retained across recovery.
    pub last_diff_id: Option<u64>,
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

pub fn init_logging() -> (WorkerGuard, ErrorCounter) {
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(crate::config::LOG_RECORDS)
        .lossy(true)
        .finish(std::io::stderr());
    let errors = writer.error_counter();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .event_format(TextFormat {
            color: std::io::stderr().is_terminal(),
        })
        .init();
    (guard, errors)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RobotMetrics {
    pub binance_skipped_publications: u64,
    pub aster_skipped_publications: u64,
}

/// State belongs to the robot callback; periodic reports do not repeat INFO events.
pub fn decision_reporter() -> impl FnMut(&crate::robot::Decision) {
    let mut previous = crate::robot::Bias::Hold;
    move |d| {
        if d.bias != previous {
            tracing::info!(name: "bias_changed", bias=?d.bias, symbol=crate::config::SYMBOL, "Aster bias changed");
            previous = d.bias;
        }
        tracing::debug!(binance=?d.binance.status, aster=?d.aster.status, bias=?d.bias, "decision for Aster");
        tracing::trace!(binance=?d.binance, aster=?d.aster, edge_bps=?d.edge_bps, "decision details");
    }
}

pub fn report_feed(venue: &'static str, metrics: &mut Output<FeedMetrics>) {
    metrics.update();
    let stats = metrics.output_buffer();
    tracing::debug!(venue, state=?stats.state, last_diff_id=stats.last_diff_id, "feed state");
    tracing::trace!(
        venue,
        applied_diffs = stats.applied_diffs,
        gaps = stats.gaps,
        resyncs = stats.resyncs,
        overflows = stats.overflows,
        decode_errors = stats.decode_errors,
        coverage_lost = stats.coverage_lost,
        duplicates = stats.duplicates,
        reconnects = stats.reconnects,
        "feed metrics"
    );
}
