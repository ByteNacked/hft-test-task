use std::{cell::Cell, io::IsTerminal};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{
    EnvFilter,
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

mod deferred;
pub use deferred::decision_reporter;
pub(crate) use deferred::{FeedEvent, feed};

thread_local! {
    // Only the formatting thread overrides this; other tracing events use wall time now.
    static EVENT_TIME: Cell<Option<std::time::SystemTime>> = const { Cell::new(None) };
}

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
        let timestamp = EVENT_TIME.get().unwrap_or_else(std::time::SystemTime::now);
        let t = time::OffsetDateTime::from(timestamp);
        write!(
            writer,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            t.year(),
            u8::from(t.month()),
            t.day(),
            t.hour(),
            t.minute(),
            t.second(),
            t.microsecond()
        )?;
        write!(writer, ": ")?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        if background.is_some() {
            write!(writer, "\x1b[0m")?;
        }
        writeln!(writer)
    }
}
// Field order keeps the writer alive until every accepted event has been formatted.
pub struct LoggingGuard {
    _formatter: deferred::FormatterGuard,
    _writer: WorkerGuard,
}

pub fn init_logging() -> std::io::Result<LoggingGuard> {
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(crate::config::LOG_RECORDS)
        .lossy(true)
        .finish(std::io::stderr());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .event_format(TextFormat {
            color: std::io::stderr().is_terminal(),
        })
        .init();
    Ok(LoggingGuard {
        _formatter: deferred::init()?,
        _writer: guard,
    })
}
