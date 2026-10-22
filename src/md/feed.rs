use crate::{
    book::{BookError, BookPublisher, BookStatus},
    config,
    logging::{self, FeedEvent},
    md::{
        AdapterError, Backoff, DepthDiff, MDAdapter, ProtocolError, ReceiveTime, Snapshot,
        get_body, shutdown_requested,
        sync::{Applied, SyncedBook},
    },
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

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeedState {
    Connecting,
    Buffering,
    SnapshotLoading,
    Replaying,
    Live,
    Backoff,
    Stopped,
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
    pub(crate) fn for_adapter<P: MDAdapter>() -> Self {
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

pub(crate) async fn run<P: MDAdapter>(
    settings: Settings,
    books: BookPublisher,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), AdapterError> {
    logging::feed(P::NAME, FeedEvent::Started(std::thread::current().id()));
    let client = reqwest::Client::builder()
        .timeout(settings.http_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut backoff = Backoff::new();
    let rules = loop {
        let body = tokio::select! {
            _ = shutdown_requested(&mut shutdown) => { return Ok(()); }
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
                let delay = backoff.next(error.retry_after());
                tracing::warn!(venue = P::NAME, %error, retry_in_ms=delay.as_millis() as u64, "metadata request failed");
                let deadline = Instant::now() + delay;
                tokio::select! {
                    _ = shutdown_requested(&mut shutdown) => { return Ok(()); }
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
        incoming: Box::default(),
        scratch: Default::default(),
        books,
        state: FeedState::Connecting,
        sync_epoch: 0,
        last_diff_id: None,
        next_log: Instant::now(),
        queue: VecDeque::with_capacity(config::SYNC_MESSAGES),
        queued_bytes: 0,
        job: SnapshotJob::Idle,
        backoff,
        next_snapshot: Instant::now(),
        sync_deadline: Instant::now(),
        live_since: None,
        online: false,
    };
    let result = runner.run(&mut shutdown).await;
    // No parse worker is detached: an already running spawn_blocking cannot be cancelled.
    let offline = runner.offline();
    let drained = runner.job.drain().await;
    runner.state = FeedState::Stopped;
    runner.log_state();
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
    incoming: Box<DepthDiff>,
    scratch: super::DiffScratch,
    books: BookPublisher,
    state: FeedState,
    sync_epoch: u64,
    last_diff_id: Option<u64>,
    next_log: Instant,
    queue: VecDeque<Buffered>,
    queued_bytes: usize,
    job: SnapshotJob,
    backoff: Backoff,
    next_snapshot: Instant,
    sync_deadline: Instant,
    live_since: Option<Instant>,
    online: bool,
}
impl<P: MDAdapter> Runner<P> {
    async fn run(&mut self, shutdown: &mut watch::Receiver<bool>) -> Result<(), AdapterError> {
        self.offline()?;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.state = FeedState::Connecting;
            self.log_state();

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
                    self.begin_sync(Duration::ZERO)?;
                    logging::feed(P::NAME, FeedEvent::Connected);
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
            if error.fatal() {
                return Err(error);
            }
            self.state = FeedState::Backoff;
            self.log_state();

            self.job.cancel_http();
            let now = Instant::now();
            let deadline = (now + self.backoff.next(error.retry_after())).max(self.next_snapshot);
            Self::log_recovery(&error, "reconnect", deadline - now);
            tokio::select! {
                _ = shutdown_requested(shutdown) => return Ok(()),
                _ = sleep_until(deadline) => {}
            }
        }
    }
    #[cold]
    fn log_recovery(error: &AdapterError, action: &'static str, retry_in: Duration) {
        let retry_in_ms = retry_in.as_millis() as u64;
        if matches!(
            error,
            AdapterError::Book(BookError::PriceRange { .. })
                | AdapterError::Protocol(ProtocolError::Book(BookError::PriceRange { .. }))
        ) {
            tracing::error!(venue=P::NAME, %error, action, retry_in_ms, "book price window exceeded; reload snapshot and recenter");
        } else {
            tracing::warn!(venue=P::NAME, %error, action, retry_in_ms, "market-data recovery");
        }
    }
    fn log_state(&mut self) {
        logging::feed(
            P::NAME,
            FeedEvent::State {
                state: self.state,
                last_diff_id: self.last_diff_id,
                exchange_time: self
                    .synced
                    .book
                    .event_time()
                    .map(|time| time.exchange_time()),
            },
        );
        self.next_log = Instant::now() + config::REPORT_INTERVAL;
    }
    #[cold]
    fn offline(&mut self) -> Result<(), AdapterError> {
        self.publish(BookStatus::Offline)?;
        self.log_status(false);
        Ok(())
    }
    fn publish(&mut self, status: BookStatus) -> Result<(), AdapterError> {
        self.books
            .publish(&self.synced.book, status)
            .map_err(AdapterError::Publication)
    }

    fn log_status(&mut self, online: bool) {
        if self.online != online {
            self.online = online;
            logging::feed(P::NAME, FeedEvent::Status(online));
        }
    }
    #[cold]
    fn begin_sync(&mut self, delay: Duration) -> Result<(), AdapterError> {
        self.sync_epoch = self
            .sync_epoch
            .checked_add(1)
            .ok_or(AdapterError::EpochOverflow)?;
        self.synced.reset();
        self.queue.clear();
        self.queued_bytes = 0;
        self.live_since = None;
        self.job.cancel_http(); // A parse worker retains its old epoch until joined.
        self.next_snapshot = self.next_snapshot.max(Instant::now() + delay);
        self.sync_deadline = self.next_snapshot + self.settings.sync_timeout;
        self.state = FeedState::Buffering;
        Ok(())
    }
    #[cold]
    fn recover(&mut self, error: AdapterError) -> Result<(), AdapterError> {
        self.offline()?;
        if error.fatal() {
            return Err(error);
        }
        let delay = self.backoff.next(error.retry_after());
        self.begin_sync(delay)?;
        Self::log_recovery(
            &error,
            "resync",
            self.next_snapshot.saturating_duration_since(Instant::now()),
        );
        Ok(())
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
        P::decode_diff_into(
            frame.text.as_bytes(),
            frame.received,
            self.rules,
            &mut self.incoming,
            &mut self.scratch,
            self.synced.book.window(),
        )?;
        let final_id = self.incoming.final_id;
        match self.synced.apply(&mut self.incoming)? {
            Applied::Update => {
                self.last_diff_id = Some(final_id);
                Ok(true)
            }
            Applied::Duplicate | Applied::BeforeSnapshot => Ok(false),
        }
    }
    fn publish_online(&mut self) -> Result<(), AdapterError> {
        let received = self
            .synced
            .book
            .event_time()
            .ok_or(AdapterError::Sequence("no bridge"))?
            .platform_time();
        if Instant::now() >= Instant::from_std(received) + self.settings.watchdog {
            return Err(AdapterError::Timeout("data watchdog"));
        }
        self.publish(BookStatus::Online)?;
        self.log_status(true);
        self.live_since.get_or_insert_with(Instant::now);
        self.state = FeedState::Live;
        Ok(())
    }
    #[cold]
    fn complete(&mut self, completion: Completion) -> Result<(), AdapterError> {
        match completion {
            Completion::Body(epoch, result) => {
                self.job = SnapshotJob::Idle;
                if epoch != self.sync_epoch {
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
                if epoch != self.sync_epoch {
                    return Ok(());
                }
                match parsed.and_then(|snapshot| self.synced.load(&snapshot)) {
                    Ok(()) => {
                        self.state = FeedState::Replaying;
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
            if now >= self.next_log {
                self.log_state();
            }
            if self
                .live_since
                .is_some_and(|start| now.duration_since(start) >= config::BACKOFF_RESET)
            {
                self.backoff.reset();
                self.live_since = Some(now);
            }
            let deadline = if self.state == FeedState::Live {
                Instant::from_std(
                    self.synced
                        .book
                        .event_time()
                        .ok_or(AdapterError::Sequence("missing live time"))?
                        .platform_time(),
                ) + self.settings.watchdog
            } else {
                self.sync_deadline
            };
            if now >= deadline {
                return Err(AdapterError::Timeout(if self.state == FeedState::Live {
                    "data watchdog"
                } else {
                    "sync"
                }));
            }
            if !self.synced.has_snapshot() && self.job.is_idle() && now >= self.next_snapshot {
                self.job.start(
                    self.sync_epoch,
                    self.client.clone(),
                    self.settings.snapshot.clone(),
                );
                self.state = FeedState::SnapshotLoading;
            }
            let mut wake = deadline.min(self.next_log);
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
                            let received = ReceiveTime::now(); // Before decode, queue or replay.
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
    fn parse<P: MDAdapter>(&mut self, epoch: u64, body: Vec<u8>, rules: InstrumentRules) {
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
