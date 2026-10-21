use std::io::IsTerminal;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
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
pub fn init_logging() -> WorkerGuard {
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
    guard
}

/// Remember the last bias, so periodic reports stay at DEBUG.
pub fn decision_reporter() -> impl FnMut(&crate::robot::Decision) {
    let mut previous = crate::robot::Bias::Hold;
    move |d| {
        if d.bias != previous {
            tracing::info!(name: "bias_changed", bias=?d.bias,
                binance=%d.binance, aster=%d.aster, "Aster bias changed");
            previous = d.bias;
        }
        tracing::debug!(binance=%d.binance, aster=%d.aster, bias=?d.bias,
            skew_ms=d.skew.map(|s|s.as_millis() as u64), threshold_bps=d.threshold_bps,
            "decision for Aster");
    }
}
