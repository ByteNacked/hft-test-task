use crate::{
    book::{BookError, BookPublisher, BookStatus},
    config,
    md::{
        AdapterError, Backoff, DepthDiff, ProtocolError, ReceiveTime, Snapshot, get_body,
        shutdown_requested,
        sync::{Applied, SyncedBook},
    },
    metrics::{FeedFault, FeedMetrics, FeedState},
    numeric::InstrumentRules,
};
use futures_util::{Sink, Stream, future::BoxFuture};
use std::{
    collections::VecDeque, future::poll_fn, marker::PhantomData, pin::Pin, task::Poll,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{self, Message, Utf8Bytes, protocol::WebSocketConfig},
};
use triple_buffer::Input;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Exchange-specific decoding, selected statically for each adapter.
pub(crate) trait Protocol: Send + 'static {
    const NAME: &'static str;
    const WS_URL: &'static str;
    const METADATA_URL: &'static str;
    const SNAPSHOT_URL: &'static str;
    fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError>;
    fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError>;
    fn decode_diff(
        body: &[u8],
        received: ReceiveTime,
        rules: InstrumentRules,
    ) -> Result<DepthDiff, ProtocolError>;
}

pub(crate) struct Settings {
    pub(crate) ws: String,
    pub(crate) metadata: String,
    pub(crate) snapshot: String,
    connect_timeout: Duration,
    http_timeout: Duration,
    sync_timeout: Duration,
    watchdog: Duration,
    queue_messages: usize,
    queue_bytes: usize,
}
impl Settings {
    pub(crate) fn for_protocol<P: Protocol>() -> Self {
        Self {
            ws: P::WS_URL.into(),
            metadata: P::METADATA_URL.into(),
            snapshot: P::SNAPSHOT_URL.into(),
            connect_timeout: config::CONNECT_TIMEOUT,
            http_timeout: config::HTTP_TIMEOUT,
            sync_timeout: config::SYNC_TIMEOUT,
            watchdog: config::WATCHDOG,
            queue_messages: config::SYNC_MESSAGES,
            queue_bytes: config::SYNC_BYTES,
        }
    }
}

pub(crate) async fn run<P: Protocol>(
    settings: Settings,
    books: BookPublisher,
    mut metrics: Input<FeedMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), AdapterError> {
    let client = reqwest::Client::builder()
        .timeout(settings.http_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut stats = FeedMetrics::default();
    let mut backoff = Backoff::new();
    let rules = loop {
        let body = tokio::select! {
            _ = shutdown_requested(&mut shutdown) => { stats.state = FeedState::Stopped; metrics.write(stats); return Ok(()); }
            result = get_body(client.clone(), settings.metadata.clone(), config::METADATA_BYTES) => result,
        };
        match body {
            Ok(body) => break P::decode_metadata(&body)?,
            Err(error)
                if matches!(
                    &error,
                    AdapterError::HttpStatus {
                        status: 429 | 500..=599,
                        ..
                    } | AdapterError::Http(_)
                ) =>
            {
                tracing::warn!(venue = P::NAME, %error, "metadata request failed");
                stats.state = FeedState::Backoff;
                stats.last_fault = Some(FeedFault::Http);
                metrics.write(stats);
                let deadline = Instant::now() + backoff.next(error.retry_after());
                tokio::select! {
                    _ = shutdown_requested(&mut shutdown) => { stats.state = FeedState::Stopped; metrics.write(stats); return Ok(()); }
                    _ = sleep_until(deadline) => {}
                }
            }
            Err(error) => return Err(error),
        }
    };
    let mut runner = Runner::<P> {
        protocol: PhantomData,
        settings,
        client,
        rules,
        synced: Box::new(SyncedBook::new(rules)?),
        books,
        metrics,
        stats,
        queue: VecDeque::with_capacity(config::SYNC_MESSAGES),
        queued_bytes: 0,
        job: SnapshotJob::Idle,
        backoff,
        next_snapshot: Instant::now(),
        sync_deadline: Instant::now(),
        live_since: None,
    };
    let result = runner.run(&mut shutdown).await;
    // No parse worker is detached: an already running spawn_blocking cannot be cancelled.
    let offline = runner.offline();
    let drained = runner.job.drain().await;
    runner.stats.state = FeedState::Stopped;
    runner.metrics.write(runner.stats);
    result.and(offline).and(drained)
}

struct Buffered {
    text: Utf8Bytes,
    received: ReceiveTime,
}
struct Runner<P> {
    protocol: PhantomData<P>,
    settings: Settings,
    client: reqwest::Client,
    rules: InstrumentRules,
    synced: Box<SyncedBook>,
    books: BookPublisher,
    metrics: Input<FeedMetrics>,
    stats: FeedMetrics,
    queue: VecDeque<Buffered>,
    queued_bytes: usize,
    job: SnapshotJob,
    backoff: Backoff,
    next_snapshot: Instant,
    sync_deadline: Instant,
    live_since: Option<Instant>,
}
impl<P: Protocol> Runner<P> {
    async fn run(&mut self, shutdown: &mut watch::Receiver<bool>) -> Result<(), AdapterError> {
        self.offline()?;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.stats.state = FeedState::Connecting;
            self.metrics.write(self.stats);
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(config::MESSAGE_BYTES))
                .max_frame_size(Some(config::MESSAGE_BYTES))
                .write_buffer_size(0)
                .max_write_buffer_size(config::MESSAGE_BYTES);
            let connection = tokio::select! {
                _ = shutdown_requested(shutdown) => return Ok(()),
                result = timeout(self.settings.connect_timeout, connect_async_with_config(&self.settings.ws, Some(ws_config), true)) => {
                    result.map_err(|_| AdapterError::Timeout("connect")).and_then(|r| r.map(|(ws,_)| ws).map_err(Into::into))
                }
            };
            let error = match connection {
                Ok(mut ws) => {
                    self.stats.connection_epoch = self
                        .stats
                        .connection_epoch
                        .checked_add(1)
                        .ok_or(AdapterError::EpochOverflow)?;
                    self.begin_sync(Duration::ZERO)?;
                    tracing::info!(
                        venue = P::NAME,
                        connection_epoch = self.stats.connection_epoch,
                        "market-data connected"
                    );
                    let result = self.connected(&mut ws, shutdown).await;
                    // Publish before any potentially blocking close/backoff operation.
                    self.offline()?;
                    let _ = timeout(config::CLOSE_TIMEOUT, ws.close(None)).await;
                    match result {
                        Ok(()) => return Ok(()),
                        Err(error) => error,
                    }
                }
                Err(error) => error,
            };
            self.record_fault(&error);
            if error.fatal() {
                return Err(error);
            }
            self.stats.reconnects += 1;
            self.stats.state = FeedState::Backoff;
            self.metrics.write(self.stats);
            self.job.cancel_http();
            let deadline =
                (Instant::now() + self.backoff.next(error.retry_after())).max(self.next_snapshot);
            tokio::select! {
                _ = shutdown_requested(shutdown) => return Ok(()),
                _ = sleep_until(deadline) => {}
            }
        }
    }
    fn offline(&mut self) -> Result<(), AdapterError> {
        self.books.publish(&self.synced.book, BookStatus::Offline)?;
        Ok(())
    }
    fn begin_sync(&mut self, delay: Duration) -> Result<(), AdapterError> {
        self.stats.sync_epoch = self
            .stats
            .sync_epoch
            .checked_add(1)
            .ok_or(AdapterError::EpochOverflow)?;
        self.stats.resyncs += 1;
        self.synced.reset();
        self.queue.clear();
        self.queued_bytes = 0;
        self.live_since = None;
        self.job.cancel_http(); // A parse worker retains its old epoch until joined.
        self.next_snapshot = self.next_snapshot.max(Instant::now() + delay);
        self.sync_deadline = self.next_snapshot + self.settings.sync_timeout;
        self.stats.state = FeedState::Buffering;
        self.metrics.write(self.stats);
        Ok(())
    }
    fn recover(&mut self, error: AdapterError) -> Result<(), AdapterError> {
        self.offline()?;
        self.record_fault(&error);
        if error.fatal() {
            return Err(error);
        }
        let delay = self.backoff.next(error.retry_after());
        self.begin_sync(delay)
    }
    fn record_fault(&mut self, error: &AdapterError) {
        let fault = match error {
            AdapterError::Sequence(_) => {
                self.stats.gaps += 1;
                FeedFault::Gap
            }
            AdapterError::QueueOverflow
            | AdapterError::Protocol(ProtocolError::BodyTooLarge)
            | AdapterError::WebSocket(tungstenite::Error::Capacity(_))
            | AdapterError::Book(BookError::Capacity) => {
                self.stats.overflows += 1;
                FeedFault::Overflow
            }
            AdapterError::Book(BookError::CoverageLost | BookError::EmptySide) => {
                self.stats.coverage_lost += 1;
                FeedFault::Coverage
            }
            AdapterError::Protocol(_) => {
                self.stats.decode_errors += 1;
                FeedFault::Decode
            }
            AdapterError::Book(_) => FeedFault::Numeric,
            AdapterError::Timeout(_) => FeedFault::Timeout,
            AdapterError::Http(error) if error.is_timeout() => FeedFault::Timeout,
            AdapterError::Http(_) | AdapterError::HttpStatus { .. } => FeedFault::Http,
            _ => FeedFault::Disconnect,
        };
        self.stats.last_fault = Some(fault);
        self.metrics.write(self.stats);
        tracing::warn!(venue=P::NAME, sync_epoch=self.stats.sync_epoch, %error, "market-data recovery");
    }
    fn enqueue(&mut self, text: Utf8Bytes, received: ReceiveTime) -> Result<(), AdapterError> {
        if self.queue.len() == self.settings.queue_messages
            || text.len() > self.settings.queue_bytes - self.queued_bytes
        {
            return Err(AdapterError::QueueOverflow);
        }
        self.queued_bytes += text.len();
        self.queue.push_back(Buffered { text, received });
        Ok(())
    }
    fn apply(&mut self, frame: Buffered) -> Result<bool, AdapterError> {
        let diff = P::decode_diff(frame.text.as_bytes(), frame.received, self.rules)?;
        match self.synced.apply(diff)? {
            Applied::Update => {
                self.stats.applied_diffs += 1;
                self.stats.last_received_at = Some(frame.received.received_at());
                Ok(true)
            }
            Applied::Duplicate => {
                self.stats.duplicates += 1;
                Ok(false)
            }
            Applied::BeforeSnapshot => Ok(false),
        }
    }
    fn publish_online(&mut self) -> Result<(), AdapterError> {
        let received = self
            .synced
            .book
            .event_time()
            .ok_or(AdapterError::Sequence("no bridge"))?
            .received_at();
        if Instant::now() >= Instant::from_std(received) + self.settings.watchdog {
            return Err(AdapterError::Timeout("data watchdog"));
        }
        self.books.publish(&self.synced.book, BookStatus::Online)?;
        self.live_since.get_or_insert_with(Instant::now);
        self.stats.state = FeedState::Live;
        Ok(())
    }
    fn complete(&mut self, completion: Completion) -> Result<(), AdapterError> {
        match completion {
            Completion::Body(epoch, result) => {
                self.job = SnapshotJob::Idle;
                if epoch != self.stats.sync_epoch {
                    return Ok(());
                }
                match result {
                    Ok(body) => self.job.parse::<P>(epoch, body, self.rules),
                    Err(error) => self.recover(error)?,
                }
            }
            Completion::Parsed(epoch, result) => {
                self.job = SnapshotJob::Idle;
                // Even obsolete workers must propagate panics to supervision.
                let parsed = result?;
                if epoch != self.stats.sync_epoch {
                    self.stats.stale_snapshots += 1;
                    return Ok(());
                }
                match parsed.and_then(|mut snapshot| self.synced.load(&mut snapshot)) {
                    Ok(()) => {
                        self.stats.snapshots += 1;
                        self.stats.state = FeedState::Replaying;
                    }
                    Err(error) => self.recover(error)?,
                }
            }
        }
        Ok(())
    }
    async fn connected(
        &mut self,
        ws: &mut Socket,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), AdapterError> {
        let mut flush_since = None;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let now = Instant::now();
            if self
                .live_since
                .is_some_and(|start| now.duration_since(start) >= config::BACKOFF_RESET)
            {
                self.backoff.reset();
                self.live_since = Some(now);
            }
            let deadline = if self.stats.state == FeedState::Live {
                Instant::from_std(
                    self.synced
                        .book
                        .event_time()
                        .ok_or(AdapterError::Sequence("missing live time"))?
                        .received_at(),
                ) + self.settings.watchdog
            } else {
                self.sync_deadline
            };
            if now >= deadline {
                return Err(AdapterError::Timeout(
                    if self.stats.state == FeedState::Live {
                        "data watchdog"
                    } else {
                        "sync"
                    },
                ));
            }
            if !self.synced.has_snapshot() && self.job.is_idle() && now >= self.next_snapshot {
                self.job.start(
                    self.stats.sync_epoch,
                    self.client.clone(),
                    self.settings.snapshot.clone(),
                );
                self.stats.state = FeedState::SnapshotLoading;
                self.metrics.write(self.stats);
            }
            let mut wake = deadline;
            if self.job.is_idle() && !self.synced.has_snapshot() {
                wake = wake.min(self.next_snapshot);
            }
            if let Some(since) = flush_since {
                let flush_deadline = since + self.settings.connect_timeout;
                if now >= flush_deadline {
                    return Err(AdapterError::Timeout("WebSocket flush"));
                }
                wake = wake.min(flush_deadline);
            }
            tokio::select! {
                _ = shutdown_requested(shutdown) => return Ok(()),
                _ = sleep_until(wake) => {},
                completion = self.job.next(), if !self.job.is_idle() => {
                    self.complete(completion)?;
                }
                message = next_message(ws, &mut flush_since) => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            let received = ReceiveTime::now()?; // Before decode, queue or replay.
                            self.stats.frames += 1;
                            let result = if self.synced.has_snapshot() && self.queue.is_empty() {
                                self.apply(Buffered { text, received }).and_then(|updated| if updated { self.publish_online() } else { Ok(()) })
                            } else { self.enqueue(text, received) };
                            if let Err(error) = result { self.recover(error)?; }
                        }
                        Some(Ok(Message::Ping(_))) => { flush_since.get_or_insert_with(Instant::now); }
                        Some(Ok(Message::Pong(_))) => {},
                        Some(Ok(Message::Close(_))) | None => return Err(AdapterError::Disconnected),
                        Some(Err(error)) => return Err(error.into()),
                        Some(Ok(_)) => self.recover(ProtocolError::WrongStream.into())?,
                    }
                }
                _ = tokio::task::yield_now(), if self.synced.has_snapshot() && !self.queue.is_empty() => {
                    let mut failed = false;
                    for _ in 0..config::REPLAY_BATCH {
                        let Some(frame) = self.queue.pop_front() else { break; };
                        self.queued_bytes -= frame.text.len();
                        if let Err(error) = self.apply(frame) { self.recover(error)?; failed = true; break; }
                    }
                    if !failed && self.queue.is_empty() && self.synced.ready()
                        && let Err(error) = self.publish_online() { self.recover(error)?; }
                }
            }
            self.metrics.write(self.stats);
        }
    }
}

// Stream buffers and pending control writes live inside ws, not this cancellable future.
// Polling next/flush again after select cancellation resumes the same operation. A Ping
// queues tungstenite's automatic Pong; flushing it must not suspend WS reads or HTTP.
async fn next_message(
    ws: &mut Socket,
    flush_since: &mut Option<Instant>,
) -> Option<Result<Message, tungstenite::Error>> {
    poll_fn(|cx| {
        if flush_since.is_some() {
            match Pin::new(&mut *ws).poll_flush(cx) {
                Poll::Ready(Ok(())) => *flush_since = None,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Pending => {}
            }
        }
        Pin::new(&mut *ws).poll_next(cx)
    })
    .await
}

enum SnapshotJob {
    Idle,
    Http {
        epoch: u64,
        request: BoxFuture<'static, Result<Vec<u8>, AdapterError>>,
    },
    Parse {
        epoch: u64,
        worker: JoinHandle<Result<Box<Snapshot>, AdapterError>>,
    },
}
enum Completion {
    Body(u64, Result<Vec<u8>, AdapterError>),
    Parsed(
        u64,
        Result<Result<Box<Snapshot>, AdapterError>, tokio::task::JoinError>,
    ),
}
impl SnapshotJob {
    fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
    fn start(&mut self, epoch: u64, client: reqwest::Client, url: String) {
        *self = Self::Http {
            epoch,
            request: Box::pin(get_body(client, url, config::MESSAGE_BYTES)),
        };
    }
    fn parse<P: Protocol>(&mut self, epoch: u64, body: Vec<u8>, rules: InstrumentRules) {
        *self = Self::Parse {
            epoch,
            worker: tokio::task::spawn_blocking(move || {
                P::decode_snapshot(&body, rules)
                    .map(Box::new)
                    .map_err(Into::into)
            }),
        };
    }
    fn cancel_http(&mut self) {
        if matches!(self, Self::Http { .. }) {
            *self = Self::Idle;
        }
    }
    async fn next(&mut self) -> Completion {
        match self {
            Self::Idle => std::future::pending().await,
            Self::Http { epoch, request } => Completion::Body(*epoch, request.await),
            Self::Parse { epoch, worker } => Completion::Parsed(*epoch, worker.await),
        }
    }
    async fn drain(&mut self) -> Result<(), AdapterError> {
        if let Self::Parse { worker, .. } = std::mem::replace(self, Self::Idle) {
            let _ = worker.await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
