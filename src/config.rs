use crate::numeric::Qty;
use clap::Parser;
use std::time::Duration;
use thiserror::Error;

pub const SYMBOL: &str = "ETHUSDT";
pub const STREAM: &str = "ethusdt@depth@100ms";
pub const SNAPSHOT_LEVELS: usize = 1000;
pub const BOOK_LEVELS: usize = 8192;
pub const PUBLISHED_LEVELS: usize = 20;
pub const DIFF_LEVELS: usize = 4096;
pub const MESSAGE_BYTES: usize = 1024 * 1024;
pub const METADATA_BYTES: usize = 8 * MESSAGE_BYTES;
pub const SYNC_MESSAGES: usize = 2048;
pub const SYNC_BYTES: usize = 8 * MESSAGE_BYTES;
pub const LOG_RECORDS: usize = 4096;
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
pub const REPLAY_BATCH: usize = 32;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(15);
pub const WATCHDOG: Duration = Duration::from_secs(3);
pub const BACKOFF_MIN: Duration = Duration::from_millis(250);
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);
pub const BACKOFF_RESET: Duration = Duration::from_secs(30);
pub const SUPERVISION_INTERVAL: Duration = Duration::from_millis(10);
pub const REPORT_INTERVAL: Duration = Duration::from_secs(1);
pub const ROBOT_LEVELS: usize = 5;

pub const DEFAULT_MAX_AGE_MS: u64 = 500;
pub const DEFAULT_MAX_SKEW_MS: u64 = 250;
pub const DEFAULT_MIN_DEPTH_QTY: Qty = Qty::ONE;
pub const DEFAULT_THRESHOLD_BPS: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid configuration: {0}")]
pub struct ConfigError(pub &'static str);

#[derive(Debug, Clone, Copy)]
pub struct RobotConfig {
    max_age: Duration,
    max_skew: Duration,
    min_depth_qty: Qty,
    threshold_bps: u32,
}
impl Default for RobotConfig {
    fn default() -> Self {
        Self {
            max_age: Duration::from_millis(DEFAULT_MAX_AGE_MS),
            max_skew: Duration::from_millis(DEFAULT_MAX_SKEW_MS),
            min_depth_qty: DEFAULT_MIN_DEPTH_QTY,
            threshold_bps: DEFAULT_THRESHOLD_BPS,
        }
    }
}
impl RobotConfig {
    pub fn new(
        max_age: Duration,
        max_skew: Duration,
        min_depth_qty: Qty,
        threshold_bps: u32,
    ) -> Result<Self, ConfigError> {
        if max_age.is_zero() || max_age > WATCHDOG {
            return Err(ConfigError("max age must be in (0, 3s]"));
        }
        if max_skew > max_age {
            return Err(ConfigError("max skew must not exceed max age"));
        }
        if min_depth_qty == Qty::ZERO {
            return Err(ConfigError("minimum depth must be positive"));
        }
        if threshold_bps == 0 || threshold_bps > 10_000 {
            return Err(ConfigError("threshold must be in 1..=10000 bps"));
        }
        Ok(Self {
            max_age,
            max_skew,
            min_depth_qty,
            threshold_bps,
        })
    }
    pub fn max_age(self) -> Duration {
        self.max_age
    }
    pub fn max_skew(self) -> Duration {
        self.max_skew
    }
    pub fn min_depth_qty(self) -> Qty {
        self.min_depth_qty
    }
    pub fn threshold_bps(self) -> u32 {
        self.threshold_bps
    }
}

#[derive(Debug, Parser)]
#[command(version, about = "Binance + Aster ETHUSDT market-data research")]
pub struct Cli {
    #[arg(long, env = "HFT_MAX_AGE_MS", default_value_t = DEFAULT_MAX_AGE_MS)]
    pub max_age_ms: u64,
    #[arg(long, env = "HFT_MAX_SKEW_MS", default_value_t = DEFAULT_MAX_SKEW_MS)]
    pub max_skew_ms: u64,
    #[arg(long, env = "HFT_MIN_DEPTH_QTY", default_value_t = DEFAULT_MIN_DEPTH_QTY)]
    pub min_depth_qty: Qty,
    #[arg(long, env = "HFT_THRESHOLD_BPS", default_value_t = DEFAULT_THRESHOLD_BPS)]
    pub threshold_bps: u32,
}
impl Cli {
    pub fn robot_config(&self) -> Result<RobotConfig, ConfigError> {
        RobotConfig::new(
            Duration::from_millis(self.max_age_ms),
            Duration::from_millis(self.max_skew_ms),
            self.min_depth_qty,
            self.threshold_bps,
        )
    }
}
