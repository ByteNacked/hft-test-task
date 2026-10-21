use crate::{
    book::{BookError, BookPublisher},
    config,
    md::{DepthDiff, ProtocolError, ReceiveTime, Snapshot},
    numeric::InstrumentRules,
};
use std::{
    future::Future,
    time::{Duration, SystemTime},
};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("HTTP {status}; retry after {retry_after:?}")]
    HttpStatus {
        status: u16,
        retry_after: Option<Duration>,
    },
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Book(#[from] BookError),
    #[error(transparent)]
    Publication(BookError),
    #[error("{0} timeout")]
    Timeout(&'static str),
    #[error("WebSocket disconnected")]
    Disconnected,
    #[error("bounded sync queue overflow")]
    QueueOverflow,
    #[error("invalid exchange sequence: {0}")]
    Sequence(&'static str),
    #[error("epoch counter overflow")]
    EpochOverflow,
    #[error("snapshot worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
}
impl AdapterError {
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::HttpStatus { retry_after, .. } => *retry_after,
            Self::WebSocket(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                retry_after(response.headers())
            }
            _ => None,
        }
    }
    pub(crate) fn fatal(&self) -> bool {
        matches!(
            self,
            Self::Publication(_) | Self::EpochOverflow | Self::Worker(_)
        )
    }
}

pub trait MDAdapter: Send + Sized + 'static {
    const NAME: &'static str;
    const WS_URL: &'static str;
    const METADATA_URL: &'static str;
    const SNAPSHOT_URL: &'static str;
    fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError>;
    fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError>;
    /// Reuses the output's bounded arrays. On error, its contents must not be applied.
    fn decode_diff_into(
        body: &[u8],
        received: ReceiveTime,
        rules: InstrumentRules,
        out: &mut DepthDiff,
    ) -> Result<(), ProtocolError>;
    fn run(
        self,
        books: BookPublisher,
        shutdown: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send;
}

/// watch::changed is cancellation-safe. Dropping the last sender also stops the worker.
pub(crate) async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Bound the body while reading chunks, before JSON parsing; no unbounded bytes().await.
pub(crate) async fn get_body(
    client: reqwest::Client,
    url: String,
    limit: usize,
) -> Result<Vec<u8>, AdapterError> {
    let mut response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(AdapterError::HttpStatus {
            status: response.status().as_u16(),
            retry_after: retry_after(response.headers()),
        });
    }
    if response
        .content_length()
        .is_some_and(|len| len > limit as u64)
    {
        return Err(ProtocolError::BodyTooLarge.into());
    }
    let mut body = Vec::with_capacity(limit);
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit - body.len() {
            return Err(ProtocolError::BodyTooLarge.into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let delay = value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()
                .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
        })?;
    // Reject durations that cannot be represented by a timer; caller uses normal backoff.
    tokio::time::Instant::now()
        .checked_add(delay)
        .map(|_| delay)
}

pub(crate) struct Backoff {
    base: Duration,
    rng: fastrand::Rng,
}
impl Backoff {
    pub(crate) fn new() -> Self {
        Self {
            base: config::BACKOFF_MIN,
            rng: fastrand::Rng::new(),
        }
    }
    pub(crate) fn reset(&mut self) {
        self.base = config::BACKOFF_MIN;
    }
    pub(crate) fn next(&mut self, retry_after: Option<Duration>) -> Duration {
        let ms = self.base.as_millis() as u64;
        let delay = Duration::from_millis(self.rng.u64(ms / 2..=ms));
        self.base = (self.base * 2).min(config::BACKOFF_MAX);
        delay.max(retry_after.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    #[test]
    fn retry_policy_bounds_growth_reset_and_server_delay() {
        let mut backoff = Backoff::new();
        let mut base = config::BACKOFF_MIN;
        for _ in 0..16 {
            let delay = backoff.next(None);
            assert!(delay >= base / 2 && delay <= base);
            base = (base * 2).min(config::BACKOFF_MAX);
        }
        backoff.reset();
        assert!(backoff.next(None) <= config::BACKOFF_MIN);
        let server_delay = Duration::from_secs(60);
        assert_eq!(backoff.next(Some(server_delay)), server_delay);
        let mut headers = reqwest::header::HeaderMap::new();
        let date = httpdate::fmt_http_date(SystemTime::now() + server_delay);
        headers.insert(reqwest::header::RETRY_AFTER, date.parse().unwrap());
        let delay = retry_after(&headers).unwrap();
        assert!(delay >= Duration::from_secs(58) && delay <= server_delay);
        headers.insert(reqwest::header::RETRY_AFTER, "invalid".parse().unwrap());
        assert_eq!(retry_after(&headers), None);
    }
    #[tokio::test]
    async fn http_body_is_bounded_with_and_without_content_length() {
        for chunked in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                let response = if chunked {
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n12345678"
                };
                socket.write_all(response.as_bytes()).await.unwrap();
            });
            let result = get_body(reqwest::Client::new(), format!("http://{addr}"), 7).await;
            assert!(matches!(
                result,
                Err(AdapterError::Protocol(ProtocolError::BodyTooLarge))
            ));
            server.await.unwrap();
        }
    }
}
