use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

struct Request {
    path: String,
    reply: oneshot::Sender<Response>,
}
#[derive(Debug)]
pub(super) struct Response {
    pub(super) status: u16,
    pub(super) headers: &'static str,
    pub(super) body: Vec<u8>,
}
impl Response {
    pub(super) fn ok(body: &[u8]) -> Self {
        Self {
            status: 200,
            headers: "",
            body: body.to_vec(),
        }
    }
}
pub(super) struct Mock {
    pub(super) settings: Settings,
    requests: mpsc::UnboundedReceiver<Request>,
    sockets: mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>,
    http: JoinHandle<()>,
    ws: JoinHandle<()>,
}
impl Mock {
    pub(super) async fn new<P: MDAdapter>() -> Self {
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
                ..Settings::for_adapter::<P>()
            },
            requests,
            sockets,
            http,
            ws,
        }
    }
    pub(super) async fn request(&mut self, path: &str) -> oneshot::Sender<Response> {
        let request = timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.path, path);
        request.reply
    }
    pub(super) async fn socket(&mut self) -> WebSocketStream<TcpStream> {
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
