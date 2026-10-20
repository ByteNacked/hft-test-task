use super::*;
use crate::{book, md::spawn_adapter, metrics};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

const METADATA: &[u8] = include_bytes!("../../../tests/fixtures/binance/metadata.json");
const SNAPSHOT: &[u8] = include_bytes!("../../../tests/fixtures/binance/snapshot.json");

struct Request {
    path: String,
    reply: oneshot::Sender<Response>,
}
#[derive(Debug)]
struct Response {
    status: u16,
    headers: &'static str,
    body: Vec<u8>,
}
impl Response {
    fn ok(body: &[u8]) -> Self {
        Self {
            status: 200,
            headers: "",
            body: body.to_vec(),
        }
    }
}
struct Mock {
    settings: Settings,
    requests: mpsc::UnboundedReceiver<Request>,
    sockets: mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>,
    http: JoinHandle<()>,
    ws: JoinHandle<()>,
}
impl Mock {
    async fn new() -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws.local_addr().unwrap();
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let (sockets_tx, sockets) = mpsc::unbounded_channel();
        let http = tokio::spawn(async move {
            loop {
                let (mut socket, _) = http.accept().await.unwrap();
                let requests_tx = requests_tx.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut buffer = [0; 1024];
                    while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buffer[..n]);
                        assert!(bytes.len() < 8192);
                    }
                    let head = std::str::from_utf8(&bytes).unwrap();
                    let path = head.split_whitespace().nth(1).unwrap().to_owned();
                    let (reply, answer) = oneshot::channel();
                    if requests_tx.send(Request { path, reply }).is_err() {
                        return;
                    }
                    let Ok(response) = answer.await else {
                        return;
                    };
                    let head = format!(
                        "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
                        response.status,
                        response.body.len(),
                        response.headers
                    );
                    if socket.write_all(head.as_bytes()).await.is_ok() {
                        let _ = socket.write_all(&response.body).await;
                    }
                });
            }
        });
        let ws = tokio::spawn(async move {
            loop {
                let (socket, _) = ws.accept().await.unwrap();
                let sockets_tx = sockets_tx.clone();
                tokio::spawn(async move {
                    if let Ok(socket) = tokio_tungstenite::accept_async(socket).await {
                        let _ = sockets_tx.send(socket);
                    }
                });
            }
        });
        Self {
            settings: Settings {
                ws: format!("ws://{ws_addr}/stream"),
                metadata: format!("http://{http_addr}/metadata"),
                snapshot: format!("http://{http_addr}/snapshot"),
                ..Settings::default()
            },
            requests,
            sockets,
            http,
            ws,
        }
    }
    async fn request(&mut self, path: &str) -> oneshot::Sender<Response> {
        let request = timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.path, path);
        request.reply
    }
    async fn socket(&mut self) -> WebSocketStream<TcpStream> {
        timeout(Duration::from_secs(3), self.sockets.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.http.abort();
        self.ws.abort();
    }
}

fn event(first: u64, last: u64, previous: u64, quantity: &str) -> Message {
    Message::Text(format!(r#"{{"stream":"ethusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000002,"T":1700000000001,"s":"ETHUSDT","U":{first},"u":{last},"pu":{previous},"b":[["3499","{quantity}"]],"a":[["3501","{quantity}"]]}}}}"#).into())
}
async fn wait_book(reader: &mut book::BookReader, status: BookStatus, after: u64) {
    timeout(Duration::from_secs(3), async {
        loop {
            reader.update();
            if reader.view().sequence() > after && reader.view().status() == status {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_metrics(
    reader: &mut triple_buffer::Output<FeedMetrics>,
    predicate: impl Fn(&FeedMetrics) -> bool,
) {
    timeout(Duration::from_secs(3), async {
        loop {
            reader.update();
            if predicate(reader.output_buffer()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn slow_snapshot_keeps_websocket_alive_then_gap_recovers_without_refreshing_duplicates() {
    let mut mock = Mock::new().await;
    let settings = std::mem::take(&mut mock.settings);
    let (books, mut reader) = book::channel(config::BOOK_LEVELS).unwrap();
    let (metrics, mut stats) = metrics::channel();
    let (stop, shutdown) = watch::channel(false);
    // Exercise the real dedicated OS thread + current-thread runtime contract.
    let worker = spawn_adapter(
        "binance-test",
        BinanceAdapter { settings },
        books,
        metrics,
        shutdown,
    )
    .unwrap();
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let mut ws = mock.socket().await;
    let snapshot = mock.request("/snapshot").await;
    ws.send(event(100, 102, 99, "0.3")).await.unwrap();
    ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(vec![1, 2, 3].into())
    );
    wait_metrics(&mut stats, |s| s.frames == 1).await;
    let before_snapshot = std::time::Instant::now();
    assert_eq!(reader.view().status(), BookStatus::Offline);
    snapshot.send(Response::ok(SNAPSHOT)).unwrap();
    wait_book(&mut reader, BookStatus::Online, 0).await;
    let sequence = reader.view().sequence();
    let received = reader.view().received_at();
    assert!(received.unwrap() <= before_snapshot);
    assert_eq!(reader.view().exchange_time(), Some(1_700_000_000_002));
    ws.send(event(100, 102, 99, "0.3")).await.unwrap();
    wait_metrics(&mut stats, |s| s.duplicates == 1).await;
    reader.update();
    assert_eq!(reader.view().sequence(), sequence);
    assert_eq!(reader.view().received_at(), received);
    ws.send(event(103, 104, 101, "0.4")).await.unwrap();
    wait_book(&mut reader, BookStatus::Offline, sequence).await;
    assert_eq!(reader.view().received_at(), received);
    let snapshot = mock.request("/snapshot").await;
    ws.send(event(100, 105, 99, "0.5")).await.unwrap();
    snapshot.send(Response::ok(SNAPSHOT)).unwrap();
    let offline = reader.view().sequence();
    wait_book(&mut reader, BookStatus::Online, offline).await;
    assert_eq!(reader.view().bids()[0].qty.raw(), 500_000_000);
    stop.send(true).unwrap();
    wait_book(&mut reader, BookStatus::Offline, offline).await;
    timeout(Duration::from_secs(3), async {
        while !worker.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    worker.join().unwrap().unwrap();
    stats.update();
    assert_eq!(stats.output_buffer().gaps, 1);
    assert_eq!(stats.output_buffer().connection_epoch, 1);
}

struct Harness {
    reader: book::BookReader,
    stats: triple_buffer::Output<FeedMetrics>,
    stop: watch::Sender<bool>,
    worker: std::thread::JoinHandle<Result<(), AdapterError>>,
}
impl Harness {
    fn start(settings: Settings) -> Self {
        let (books, reader) = book::channel(config::BOOK_LEVELS).unwrap();
        let (metrics, stats) = metrics::channel();
        let (stop, shutdown) = watch::channel(false);
        let worker = spawn_adapter(
            "binance-test",
            BinanceAdapter { settings },
            books,
            metrics,
            shutdown,
        )
        .unwrap();
        Self {
            reader,
            stats,
            stop,
            worker,
        }
    }
    async fn live(&mut self, mock: &mut Mock, ws: &mut WebSocketStream<TcpStream>) {
        let snapshot = mock.request("/snapshot").await;
        ws.send(event(100, 102, 99, "0.3")).await.unwrap();
        snapshot.send(Response::ok(SNAPSHOT)).unwrap();
        let after = self.reader.view().sequence();
        wait_book(&mut self.reader, BookStatus::Online, after).await;
    }
    async fn finish(mut self) {
        self.stop.send(true).unwrap();
        timeout(Duration::from_secs(2), async {
            while !self.worker.is_finished() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        self.worker.join().unwrap().unwrap();
        self.reader.update();
        assert_eq!(self.reader.view().status(), BookStatus::Offline);
        self.stats.update();
        assert_eq!(self.stats.output_buffer().state, FeedState::Stopped);
    }
}

#[tokio::test]
async fn malformed_frame_and_both_close_and_eof_recover() {
    let mut mock = Mock::new().await;
    let mut h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let mut ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    let seq = h.reader.view().sequence();
    ws.send(Message::Text("{broken".into())).await.unwrap();
    wait_book(&mut h.reader, BookStatus::Offline, seq).await;
    wait_metrics(&mut h.stats, |s| s.decode_errors == 1).await;
    h.live(&mut mock, &mut ws).await;
    let seq = h.reader.view().sequence();
    ws.close(None).await.unwrap();
    wait_book(&mut h.reader, BookStatus::Offline, seq).await;
    ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    let seq = h.reader.view().sequence();
    drop(ws);
    wait_book(&mut h.reader, BookStatus::Offline, seq).await;
    let mut ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    wait_metrics(&mut h.stats, |s| s.connection_epoch == 3).await;
    h.finish().await;
}

#[tokio::test]
async fn queue_count_and_byte_limits_cancel_old_http_and_recover() {
    for bytes_limit in [false, true] {
        let mut mock = Mock::new().await;
        if bytes_limit {
            mock.settings.queue_bytes = 1;
        } else {
            mock.settings.queue_messages = 1;
        }
        let mut h = Harness::start(std::mem::take(&mut mock.settings));
        mock.request("/metadata")
            .await
            .send(Response::ok(METADATA))
            .unwrap();
        let mut ws = mock.socket().await;
        let old = mock.request("/snapshot").await;
        ws.send(event(100, 102, 99, "0.3")).await.unwrap();
        if !bytes_limit {
            ws.send(event(103, 104, 102, "0.4")).await.unwrap();
        }
        wait_metrics(&mut h.stats, |s| s.overflows == 1).await;
        h.reader.update();
        assert_eq!(h.reader.view().status(), BookStatus::Offline);
        // The old response may arrive after resync, but must never become a snapshot.
        old.send(Response::ok(SNAPSHOT)).unwrap();
        let next = mock.request("/snapshot").await;
        next.send(Response::ok(SNAPSHOT)).unwrap();
        wait_metrics(&mut h.stats, |s| s.snapshots == 1).await;
        // With snapshot ready, the live bridge bypasses the bounded sync queue.
        ws.send(event(100, 105, 99, "0.5")).await.unwrap();
        wait_book(&mut h.reader, BookStatus::Online, 0).await;
        assert_eq!(h.reader.view().bids()[0].qty.raw(), 500_000_000);
        h.finish().await;
    }
}

#[tokio::test]
async fn watchdog_does_not_accept_ping_or_duplicate_as_fresh_data() {
    let mut mock = Mock::new().await;
    mock.settings.watchdog = Duration::from_secs(1);
    let mut h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let mut ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    let seq = h.reader.view().sequence();
    let received = h.reader.view().received_at();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        ws.send(event(100, 102, 99, "0.3")).await.unwrap();
        ws.send(Message::Ping(vec![42].into())).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Pong(vec![42].into())
        );
    }
    wait_book(&mut h.reader, BookStatus::Offline, seq).await;
    assert_eq!(h.reader.view().received_at(), received);
    assert!(received.unwrap().elapsed() < Duration::from_millis(1400));
    wait_metrics(&mut h.stats, |s| s.last_fault == Some(FeedFault::Timeout)).await;
    let mut ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    h.finish().await;
}

#[tokio::test]
async fn http_and_sync_deadlines_recover_and_shutdown_cancels_pending_http() {
    for http_timeout in [true, false] {
        let mut mock = Mock::new().await;
        if http_timeout {
            mock.settings.http_timeout = Duration::from_millis(150);
        } else {
            mock.settings.sync_timeout = Duration::from_millis(150);
        }
        let mut h = Harness::start(std::mem::take(&mut mock.settings));
        mock.request("/metadata")
            .await
            .send(Response::ok(METADATA))
            .unwrap();
        let mut ws = mock.socket().await;
        let pending = mock.request("/snapshot").await;
        wait_metrics(&mut h.stats, |s| s.last_fault == Some(FeedFault::Timeout)).await;
        drop(pending);
        if !http_timeout {
            ws = mock.socket().await;
        }
        h.live(&mut mock, &mut ws).await;
        h.finish().await;
    }
    let mut mock = Mock::new().await;
    let h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let _ws = mock.socket().await;
    let _pending = mock.request("/snapshot").await;
    h.finish().await;
}

#[tokio::test]
async fn retry_after_prevents_early_retry_and_shutdown_interrupts_backoff() {
    let mut mock = Mock::new().await;
    let mut h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let _ws = mock.socket().await;
    let reply = mock.request("/snapshot").await;
    let start = Instant::now();
    reply
        .send(Response {
            status: 429,
            headers: "Retry-After: 1\r\n",
            body: Vec::new(),
        })
        .unwrap();
    wait_metrics(&mut h.stats, |s| s.last_fault == Some(FeedFault::Http)).await;
    let reply = mock.request("/snapshot").await;
    assert!(start.elapsed() >= Duration::from_secs(1));
    reply
        .send(Response {
            status: 429,
            headers: "Retry-After: 60\r\n",
            body: Vec::new(),
        })
        .unwrap();
    wait_metrics(&mut h.stats, |s| s.resyncs == 3).await;
    h.finish().await;
}

#[tokio::test]
async fn websocket_handshake_timeout_is_retriable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut mock = Mock::new().await;
    mock.settings.ws = format!("ws://{}", listener.local_addr().unwrap());
    mock.settings.connect_timeout = Duration::from_millis(100);
    let mut h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let (_stalled, _) = listener.accept().await.unwrap();
    wait_metrics(&mut h.stats, |s| s.last_fault == Some(FeedFault::Timeout)).await;
    let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
    h.live(&mut mock, &mut ws).await;
    h.finish().await;
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

#[tokio::test]
async fn old_parse_worker_is_retained_joined_and_its_snapshot_is_discarded() {
    let rules = super::super::decode_metadata(METADATA).unwrap();
    let (books, mut reader) = book::channel(config::BOOK_LEVELS).unwrap();
    let (metrics, _) = metrics::channel();
    let mut runner = Runner {
        settings: Settings::default(),
        client: reqwest::Client::new(),
        rules,
        synced: Box::new(SyncedBook::new(rules).unwrap()),
        books,
        metrics,
        stats: FeedMetrics::default(),
        queue: VecDeque::with_capacity(config::SYNC_MESSAGES),
        queued_bytes: 0,
        job: SnapshotJob::Idle,
        backoff: Backoff::new(),
        next_snapshot: Instant::now(),
        sync_deadline: Instant::now(),
        live_since: None,
    };
    runner.begin_sync(Duration::ZERO).unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let (started, ready) = oneshot::channel();
    runner.job = SnapshotJob::Parse {
        epoch: runner.stats.sync_epoch,
        worker: tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            super::super::decode_snapshot(SNAPSHOT, rules)
                .map(Box::new)
                .map_err(Into::into)
        }),
    };
    ready.await.unwrap();
    runner.recover(AdapterError::QueueOverflow).unwrap();
    assert!(!runner.job.is_idle());
    assert!(
        timeout(Duration::from_millis(10), runner.job.next())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let completion = runner.job.next().await;
    runner.complete(completion).unwrap();
    assert_eq!(runner.stats.stale_snapshots, 1);
    assert!(!runner.synced.has_snapshot());
    reader.update();
    assert_eq!(reader.view().status(), BookStatus::Offline);
    runner
        .job
        .parse(runner.stats.sync_epoch, SNAPSHOT.to_vec(), rules);
    let completion = runner.job.next().await;
    runner.complete(completion).unwrap();
    assert_eq!(runner.stats.snapshots, 1);
    assert!(runner.synced.has_snapshot());
    // Shutdown also waits for an uncancellable parse, instead of detaching it.
    let (release, wait) = std::sync::mpsc::channel();
    runner.job = SnapshotJob::Parse {
        epoch: runner.stats.sync_epoch,
        worker: tokio::task::spawn_blocking(move || {
            wait.recv().unwrap();
            Ok(Box::new(
                super::super::decode_snapshot(SNAPSHOT, rules).unwrap(),
            ))
        }),
    };
    let drain = runner.job.drain();
    tokio::pin!(drain);
    assert!(
        timeout(Duration::from_millis(10), &mut drain)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    drain.await.unwrap();
}

#[tokio::test]
async fn oversized_snapshot_and_websocket_message_fail_closed_then_recover() {
    let mut mock = Mock::new().await;
    let mut h = Harness::start(std::mem::take(&mut mock.settings));
    mock.request("/metadata")
        .await
        .send(Response::ok(METADATA))
        .unwrap();
    let mut ws = mock.socket().await;
    mock.request("/snapshot")
        .await
        .send(Response::ok(&vec![b' '; config::MESSAGE_BYTES + 1]))
        .unwrap();
    wait_metrics(&mut h.stats, |s| s.overflows == 1).await;
    h.reader.update();
    assert_eq!(h.reader.view().status(), BookStatus::Offline);
    h.live(&mut mock, &mut ws).await;
    let seq = h.reader.view().sequence();
    let _ = ws
        .send(Message::Text(" ".repeat(config::MESSAGE_BYTES + 1).into()))
        .await;
    wait_book(&mut h.reader, BookStatus::Offline, seq).await;
    wait_metrics(&mut h.stats, |s| s.overflows == 2).await;
    let mut ws = mock.socket().await;
    h.live(&mut mock, &mut ws).await;
    h.finish().await;
}
