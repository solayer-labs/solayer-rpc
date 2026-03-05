use std::{
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use bytes::{Buf, BufMut, Bytes};
use dashmap::{mapref::entry::Entry, DashMap};
use futures_core::Stream;
use http_body::Frame;
use http_body_util::{BodyExt, Full};
use hyper_util::{
    client::legacy::{connect::HttpConnector as LegacyHttpConnector, Client as LegacyClient},
    rt::{TokioExecutor, TokioTimer},
};
use infinisvm_types::sync::{
    CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, ShredId, SignedFinalization, SyncBatchShred,
};
use metrics::counter;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tonic::transport::server::TcpConnectInfo;

pub use super::*;

type HttpClient = LegacyClient<LegacyHttpConnector, Full<Bytes>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeTransactionBatchRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBatchShredRequest {
    pub shred_id: ShredId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBlockFinalizerRequest {
    pub slot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectCommitBatchRequest {
    pub peer_id: [u8; 32],
    pub peer_addr: String,
    pub notification: CommitBatchNotification,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectCommitBatchResponse {
    pub ok: bool,
}

#[derive(Clone, Copy, Debug)]
struct RateLimitConfig {
    rate_per_sec: u32,
    burst: u32,
}

#[derive(Debug)]
struct RateLimiterState {
    tokens: u32,
    last_refill: Instant,
}

#[derive(Debug)]
struct RateLimiter {
    rate_per_sec: u32,
    burst: u32,
    state: Mutex<RateLimiterState>,
}

impl RateLimiter {
    fn new(rate_per_sec: u32, burst: u32) -> Self {
        let burst = burst.max(1);
        Self {
            rate_per_sec,
            burst,
            state: Mutex::new(RateLimiterState {
                tokens: burst,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn allow(&self) -> bool {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill);
        let refill = (elapsed.as_secs_f64() * self.rate_per_sec as f64) as u32;
        if refill > 0 {
            state.tokens = state.tokens.saturating_add(refill).min(self.burst);
            state.last_refill = now;
        }
        if state.tokens == 0 {
            return false;
        }
        state.tokens -= 1;
        true
    }
}

fn extract_remote_ip(parts: &http::request::Parts) -> Option<IpAddr> {
    if let Some(info) = parts.extensions.get::<TcpConnectInfo>() {
        if let Some(addr) = info.remote_addr() {
            return Some(addr.ip());
        }
    }
    None
}

// Service trait
#[tonic::async_trait]
pub trait InfiniSvmService: Send + Sync + 'static + Clone {
    type SubscribeTransactionBatchesStream: Stream<Item = Result<Arc<CommitBatchNotification>, tonic::Status>>
        + Send
        + 'static;

    async fn subscribe_commit_batch_notifications(
        &self,
        request: tonic::Request<SubscribeTransactionBatchRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTransactionBatchesStream>, tonic::Status>;

    async fn get_batch_shred(
        &self,
        request: tonic::Request<GetBatchShredRequest>,
    ) -> Result<tonic::Response<SyncBatchShred>, tonic::Status>;

    async fn get_block_finalizer(
        &self,
        request: tonic::Request<GetBlockFinalizerRequest>,
    ) -> Result<tonic::Response<SignedFinalization>, tonic::Status>;

    async fn get_peer_status(
        &self,
        request: tonic::Request<GetPeerStatusRequest>,
    ) -> Result<tonic::Response<GetPeerStatusResponse>, tonic::Status>;

    async fn inject_commit_batch_notification(
        &self,
        request: tonic::Request<InjectCommitBatchRequest>,
    ) -> Result<tonic::Response<InjectCommitBatchResponse>, tonic::Status>;
}

// Server wrapper
#[derive(Clone)]
pub struct InfiniSvmServiceServer<T> {
    inner: T,
    rate_limit: Option<RateLimitConfig>,
    rate_limiters: Arc<DashMap<Option<IpAddr>, Arc<RateLimiter>>>,
}

impl<T> InfiniSvmServiceServer<T>
where
    T: InfiniSvmService,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            rate_limit: None,
            rate_limiters: Arc::new(DashMap::new()),
        }
    }

    pub fn with_rate_limit(mut self, rate_per_sec: u32, burst: u32) -> Self {
        if rate_per_sec == 0 {
            return self;
        }
        let burst = if burst == 0 { rate_per_sec } else { burst };
        self.rate_limit = Some(RateLimitConfig { rate_per_sec, burst });
        self
    }
}

impl<T> tonic::server::NamedService for InfiniSvmServiceServer<T>
where
    T: InfiniSvmService,
{
    const NAME: &'static str = "infinisvm.sync.InfiniSVMService";
}

// Implement the required Service trait for tonic compatibility
impl<T> tower::Service<http::Request<tonic::body::Body>> for InfiniSvmServiceServer<T>
where
    T: InfiniSvmService,
{
    type Response = http::Response<http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, tonic::Status>>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        use http_body_util::BodyExt;

        let inner = self.inner.clone();
        let rate_limit = self.rate_limit;
        let rate_limiters = self.rate_limiters.clone();
        let (parts, body) = req.into_parts();

        Box::pin(async move {
            let result: Result<
                http::Response<http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, tonic::Status>>,
                tonic::Status,
            > = async move {
                if let Some(rate_limit) = rate_limit {
                    let key = extract_remote_ip(&parts);
                    let limiter = match rate_limiters.entry(key) {
                        Entry::Occupied(entry) => entry.get().clone(),
                        Entry::Vacant(entry) => {
                            let limiter = Arc::new(RateLimiter::new(rate_limit.rate_per_sec, rate_limit.burst));
                            entry.insert(limiter.clone());
                            limiter
                        }
                    };
                    if !limiter.allow().await {
                        counter!("grpc_server_rate_limited_total").increment(1);
                        return Err(tonic::Status::resource_exhausted("rate limit exceeded"));
                    }
                }
                // Extract the method name from the path
                let path = parts.uri.path();
                let method_name = path.rsplit('/').next().unwrap_or("");

                // Collect the request body
                let body_bytes = {
                    use http_body_util::BodyExt;
                    let collected = body
                        .collect()
                        .await
                        .map_err(|_e| tonic::Status::internal("Failed to read body".to_string()))?;
                    collected.to_bytes()
                };

                // Skip the gRPC message framing (5 bytes: 1 byte compression flag + 4 bytes length)
                let message_bytes = if body_bytes.len() > 5 {
                    &body_bytes[5..]
                } else {
                    &body_bytes[..]
                };

                // Route to the appropriate method
                match method_name {
                    "SubscribeTransactionBatches" => {
                        // Deserialize request
                        let request: SubscribeTransactionBatchRequest =
                            bincode::deserialize(message_bytes).map_err(|e| {
                                counter!("grpc_server_errors_total", "method" => "SubscribeTransactionBatches", "kind" => "deserialize").increment(1);
                                tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                            })?;

                        // Call the service method
                        let response = inner
                            .subscribe_commit_batch_notifications(tonic::Request::new(request))
                            .await?;
                        let stream = response.into_inner();

        // For streaming responses, directly map the stream into framed bytes to avoid extra hops
        let body_stream = tokio_stream::StreamExt::map(
            stream,
            |item_res: Result<Arc<CommitBatchNotification>, tonic::Status>| match item_res {
                Ok(item) => match bincode::serialize(&item) {
                    Ok(serialized) => {
                        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                        frame.put_u8(0); // No compression
                        frame.put_u32(serialized.len() as u32);
                        frame.extend_from_slice(&serialized);
                        counter!("grpc_server_messages_total", "method" => "SubscribeTransactionBatches").increment(1);
                        counter!("grpc_server_bytes_total", "direction" => "tx", "method" => "SubscribeTransactionBatches")
                            .increment(serialized.len() as u64);
                        Ok(Frame::data(frame.freeze()))
                    }
                    Err(e) => {
                        counter!("grpc_server_errors_total", "method" => "SubscribeTransactionBatches", "kind" => "serialize").increment(1);
                        Err(tonic::Status::internal(format!(
                            "Failed to serialize response: {e}"
                        )))
                    }
                },
                Err(status) => Err(status),
            },
        );

        let body = http_body_util::StreamBody::new(body_stream);
        let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "0")
                            .body(boxed_body)
                            .unwrap();
                        Ok(response)
                    }
                    "GetTransactionBatch" => {
                        // Deserialize request
                        let request: GetBatchShredRequest = bincode::deserialize(message_bytes).map_err(|e| {
                            tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                        })?;

                        // Call the service method
                        let response = inner.get_batch_shred(tonic::Request::new(request)).await?;
                        let response_data = response.into_inner();

                        // Serialize response
                        let serialized = bincode::serialize(&response_data)
                            .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {e}")))?;

                        // Create response with gRPC message framing
                        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                        frame.put_u8(0); // No compression
                        frame.put_u32(serialized.len() as u32);
                        frame.extend_from_slice(&serialized);

                        let body = http_body_util::Full::new(frame.freeze())
                            .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error"));
                        let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "0")
                            .body(boxed_body)
                            .unwrap();
                        Ok(response)
            }
                    "GetBlockFinalizer" => {
                        // Deserialize request
                        let request: GetBlockFinalizerRequest = bincode::deserialize(message_bytes).map_err(|e| {
                            tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                        })?;

                        // Call the service method
                        let response = inner.get_block_finalizer(tonic::Request::new(request)).await?;
                        let response_data = response.into_inner();

                        // Serialize response
                        let serialized = bincode::serialize(&response_data)
                            .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {e}")))?;

                        // Create response with gRPC message framing
                        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                        frame.put_u8(0); // No compression
                        frame.put_u32(serialized.len() as u32);
                        frame.extend_from_slice(&serialized);

                        let body = http_body_util::Full::new(frame.freeze())
                            .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error"));
                        let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "0")
                            .body(boxed_body)
                            .unwrap();
                        Ok(response)
                    }
                    "GetPeerStatus" => {
                        let request: GetPeerStatusRequest = bincode::deserialize(message_bytes)
                            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize request: {e}")))?;

                        let response = inner.get_peer_status(tonic::Request::new(request)).await?;
                        let response_data = response.into_inner();

                        let serialized = bincode::serialize(&response_data)
                            .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {e}")))?;

                        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                        frame.put_u8(0);
                        frame.put_u32(serialized.len() as u32);
                        frame.extend_from_slice(&serialized);

                        let body = http_body_util::Full::new(frame.freeze())
                            .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error"));
                        let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "0")
                            .body(boxed_body)
                            .unwrap();
                        Ok(response)
                    }
                    "InjectCommitBatchNotification" => {
                        let request: InjectCommitBatchRequest = bincode::deserialize(message_bytes)
                            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize request: {e}")))?;

                        let response = inner
                            .inject_commit_batch_notification(tonic::Request::new(request))
                            .await?;
                        let response_data = response.into_inner();

                        let serialized = bincode::serialize(&response_data)
                            .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {e}")))?;

                        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                        frame.put_u8(0);
                        frame.put_u32(serialized.len() as u32);
                        frame.extend_from_slice(&serialized);

                        let body = http_body_util::Full::new(frame.freeze())
                            .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error"));
                        let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "0")
                            .body(boxed_body)
                            .unwrap();
                        Ok(response)
                    }
                    _ => {
                        // Unknown method
                        let status = tonic::Status::unimplemented(format!("Unknown method: {method_name}"));
                        counter!("grpc_server_errors_total", "kind" => "unknown_method").increment(1);
                        let response = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", status.code() as i32)
                            .header("grpc-message", status.message())
                            .body(http_body_util::combinators::UnsyncBoxBody::new(
                                http_body_util::Empty::<bytes::Bytes>::new().map_err(
                                    |_: std::convert::Infallible| tonic::Status::internal("impossible error"),
                                ),
                            ))
                            .unwrap();
                        Ok(response)
                    }
                }
            }
            .await;

            match result {
                Ok(response) => Ok(response),
                Err(status) => {
                    counter!("grpc_server_errors_total", "kind" => "handler", "code" => format!("{}", status.code()))
                        .increment(1);
                    // Convert tonic::Status to HTTP response
                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", status.code() as i32)
                        .header("grpc-message", status.message())
                        .body(http_body_util::combinators::UnsyncBoxBody::new(
                            http_body_util::Empty::<bytes::Bytes>::new()
                                .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error")),
                        ))
                        .unwrap();
                    Ok(response)
                }
            }
        })
    }
}

// Simplified client
pub struct InfiniSvmServiceClient {
    http_client: HttpClient,
    base_uri: String,
}

impl InfiniSvmServiceClient {
    pub fn new(base_uri: String) -> Self {
        let mut connector = LegacyHttpConnector::new();
        connector.enforce_http(false);

        let http_client: HttpClient = {
            let mut builder = LegacyClient::builder(TokioExecutor::new());
            builder
                .http2_only(true)
                .http2_keep_alive_interval(Some(std::time::Duration::from_secs(10)))
                .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
                .timer(TokioTimer::new());
            builder.build(connector)
        };

        Self { http_client, base_uri }
    }

    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let uri = endpoint.uri().clone();
        let mut connector = LegacyHttpConnector::new();
        connector.enforce_http(false);

        let http_client: HttpClient = {
            let mut builder = LegacyClient::builder(TokioExecutor::new());
            builder
                .http2_only(true)
                .http2_keep_alive_interval(Some(std::time::Duration::from_secs(10)))
                .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
                .timer(TokioTimer::new());
            builder.build(connector)
        };

        // Extract base URI from the endpoint
        let base_uri = format!(
            "{}://{}:{}",
            uri.scheme_str().unwrap_or("http"),
            uri.host().unwrap_or("localhost"),
            uri.port_u16().unwrap_or(5005)
        );

        Ok(Self { http_client, base_uri })
    }

    pub fn max_decoding_message_size(self, _limit: usize) -> Self {
        self
    }

    pub fn max_encoding_message_size(self, _limit: usize) -> Self {
        self
    }

    pub async fn subscribe_transaction_batches(
        &mut self,
        request: impl tonic::IntoRequest<SubscribeTransactionBatchRequest>,
    ) -> Result<tonic::Response<BincodeStreaming<CommitBatchNotification>>, tonic::Status> {
        // Create a bincode-based HTTP request
        let req = request.into_request();
        let request_data = req.into_inner();

        // Serialize request with bincode
        let serialized = bincode::serialize(&request_data)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize request: {e}")))?;

        // Add gRPC message framing
        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
        frame.put_u8(0); // No compression
        frame.put_u32(serialized.len() as u32);
        frame.extend_from_slice(&serialized);

        // Create HTTP/2 request
        let uri = format!(
            "{}/infinisvm.sync.InfiniSVMService/SubscribeTransactionBatches",
            self.base_uri
        )
        .parse::<hyper::Uri>()
        .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

        let http_request = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::from(frame.freeze()))
            .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

        // Use the shared HTTP client
        let response = self
            .http_client
            .request(http_request)
            .await
            .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

        // Check response status
        if response.status() != hyper::StatusCode::OK {
            return Err(tonic::Status::internal(format!(
                "Unexpected status: {}",
                response.status()
            )));
        }

        // Create a channel for the streaming response with larger buffer
        let (tx, rx) = tokio::sync::mpsc::channel(512);
        let body = response.into_body();

        // Spawn a task to read the streaming response
        tokio::spawn(async move {
            let mut buffer = bytes::BytesMut::new();
            let mut body = body;

            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        let bytes = match frame.into_data() {
                            Ok(bytes) => bytes,
                            Err(_) => continue,
                        };
                        buffer.extend_from_slice(&bytes);

                        // Process complete messages in the buffer
                        while buffer.len() >= 5 {
                            // Read frame header
                            let _compression = buffer[0];
                            let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;

                            // Check if we have the complete message
                            if buffer.len() >= 5 + length {
                                // Skip compression flag and length
                                let message_bytes = &buffer[5..5 + length];

                                // Deserialize the message
                                // Try new format first, then fall back to legacy without job_id
                                let decoded = bincode::deserialize::<CommitBatchNotification>(message_bytes);

                                match decoded {
                                    Ok(response) => {
                                        if tx.send(Ok(response)).await.is_err() {
                                            return;
                                        }
                                        counter!("grpc_client_messages_total", "method" => "SubscribeTransactionBatches").increment(1);
                                        counter!("grpc_client_bytes_total", "direction" => "rx", "method" => "SubscribeTransactionBatches").increment(length as u64);
                                    }
                                    Err(e) => {
                                        counter!("grpc_client_errors_total", "method" => "SubscribeTransactionBatches", "kind" => "deserialize").increment(1);
                                        let _ = tx
                                            .send(Err(tonic::Status::internal(format!(
                                                "Failed to deserialize response: {e}",
                                            ))))
                                            .await;
                                        return;
                                    }
                                }

                                // Remove processed message from buffer
                                buffer.advance(5 + length);
                            } else {
                                // Need more data
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        counter!("grpc_client_errors_total", "method" => "SubscribeTransactionBatches", "kind" => "http_body").increment(1);
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "Failed to read response body: {e}",
                            ))))
                            .await;
                        return;
                    }
                }
            }
        });

        // Create a streaming response
        let streaming = BincodeStreaming::new(rx);
        Ok(tonic::Response::new(streaming))
    }

    pub async fn get_batch_shred(
        &mut self,
        request: impl tonic::IntoRequest<GetBatchShredRequest>,
    ) -> Result<tonic::Response<SyncBatchShred>, tonic::Status> {
        let req = request.into_request();
        let request_data = req.into_inner();

        // Serialize request with bincode
        let serialized = bincode::serialize(&request_data)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize request: {e}")))?;

        // Add gRPC framing
        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
        frame.put_u8(0);
        frame.put_u32(serialized.len() as u32);
        frame.extend_from_slice(&serialized);

        let uri = format!("{}/infinisvm.sync.InfiniSVMService/GetTransactionBatch", self.base_uri)
            .parse::<hyper::Uri>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

        let http_request = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::from(frame.freeze()))
            .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

        let mut response = self
            .http_client
            .request(http_request)
            .await
            .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

        if response.status() != hyper::StatusCode::OK {
            return Err(tonic::Status::internal(format!(
                "Unexpected status: {}",
                response.status()
            )));
        }

        // Check for gRPC error status in headers (server places status in headers on
        // error)
        if let Some(gs) = response.headers().get("grpc-status") {
            let code_i32 = gs.to_str().ok().and_then(|s| s.parse::<i32>().ok()).unwrap_or(2);
            if code_i32 != 0 {
                let msg = response
                    .headers()
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                return Err(tonic::Status::new(tonic::Code::from_i32(code_i32), msg));
            }
        }

        // Collect the full response body (unary)
        let mut body_bytes = bytes::BytesMut::new();
        while let Some(frame) = response.body_mut().frame().await {
            match frame {
                Ok(frame) => {
                    let bytes = match frame.into_data() {
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    body_bytes.extend_from_slice(&bytes);
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("Body read failed: {e}")));
                }
            }
        }

        let body_bytes = body_bytes.freeze();
        if body_bytes.len() < 5 {
            // If grpc-status header indicated success but body is empty, treat as internal
            // error
            return Err(tonic::Status::internal("Response too short"));
        }
        let message_bytes = &body_bytes[5..];
        // Try new format first, then legacy fallback
        let decoded: SyncBatchShred = bincode::deserialize(message_bytes)
            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize response: {e}")))?;

        Ok(tonic::Response::new(decoded))
    }

    pub async fn get_block_finalizer(
        &mut self,
        request: impl tonic::IntoRequest<GetBlockFinalizerRequest>,
    ) -> Result<tonic::Response<SignedFinalization>, tonic::Status> {
        let req = request.into_request();
        let request_data = req.into_inner();

        let serialized = bincode::serialize(&request_data)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize request: {e}")))?;

        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
        frame.put_u8(0);
        frame.put_u32(serialized.len() as u32);
        frame.extend_from_slice(&serialized);

        let uri = format!("{}/infinisvm.sync.InfiniSVMService/GetBlockFinalizer", self.base_uri)
            .parse::<hyper::Uri>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

        let http_request = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::from(frame.freeze()))
            .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

        let mut response = self
            .http_client
            .request(http_request)
            .await
            .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

        if response.status() != hyper::StatusCode::OK {
            return Err(tonic::Status::internal(format!(
                "Unexpected status: {}",
                response.status()
            )));
        }

        if let Some(gs) = response.headers().get("grpc-status") {
            let code_i32 = gs.to_str().ok().and_then(|s| s.parse::<i32>().ok()).unwrap_or(2);
            if code_i32 != 0 {
                let msg = response
                    .headers()
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                return Err(tonic::Status::new(tonic::Code::from_i32(code_i32), msg));
            }
        }

        let mut body_bytes = bytes::BytesMut::new();
        while let Some(frame) = response.body_mut().frame().await {
            match frame {
                Ok(frame) => {
                    let bytes = match frame.into_data() {
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    body_bytes.extend_from_slice(&bytes);
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("Body read failed: {e}")));
                }
            }
        }

        let body_bytes = body_bytes.freeze();
        if body_bytes.len() < 5 {
            return Err(tonic::Status::internal("Response too short"));
        }
        let message_bytes = &body_bytes[5..];
        let decoded: SignedFinalization = bincode::deserialize(message_bytes)
            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize response: {e}")))?;

        Ok(tonic::Response::new(decoded))
    }

    pub async fn get_peer_status(
        &mut self,
        request: impl tonic::IntoRequest<GetPeerStatusRequest>,
    ) -> Result<tonic::Response<GetPeerStatusResponse>, tonic::Status> {
        let req = request.into_request();
        let request_data = req.into_inner();

        let serialized = bincode::serialize(&request_data)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize request: {e}")))?;

        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
        frame.put_u8(0);
        frame.put_u32(serialized.len() as u32);
        frame.extend_from_slice(&serialized);

        let uri = format!("{}/infinisvm.sync.InfiniSVMService/GetPeerStatus", self.base_uri)
            .parse::<hyper::Uri>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

        let http_request = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::from(frame.freeze()))
            .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

        let mut response = self
            .http_client
            .request(http_request)
            .await
            .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

        if response.status() != hyper::StatusCode::OK {
            return Err(tonic::Status::internal(format!(
                "Unexpected status: {}",
                response.status()
            )));
        }

        if let Some(gs) = response.headers().get("grpc-status") {
            let code_i32 = gs.to_str().ok().and_then(|s| s.parse::<i32>().ok()).unwrap_or(2);
            if code_i32 != 0 {
                let msg = response
                    .headers()
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                return Err(tonic::Status::new(tonic::Code::from_i32(code_i32), msg));
            }
        }

        let mut body_bytes = bytes::BytesMut::new();
        while let Some(frame) = response.body_mut().frame().await {
            match frame {
                Ok(frame) => {
                    let bytes = match frame.into_data() {
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    body_bytes.extend_from_slice(&bytes);
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("Body read failed: {e}")));
                }
            }
        }

        let body_bytes = body_bytes.freeze();
        if body_bytes.len() < 5 {
            return Err(tonic::Status::internal("Response too short"));
        }
        let message_bytes = &body_bytes[5..];
        let decoded: GetPeerStatusResponse = bincode::deserialize(message_bytes)
            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize response: {e}")))?;

        Ok(tonic::Response::new(decoded))
    }

    pub async fn inject_commit_batch_notification(
        &mut self,
        request: impl tonic::IntoRequest<InjectCommitBatchRequest>,
    ) -> Result<tonic::Response<InjectCommitBatchResponse>, tonic::Status> {
        let req = request.into_request();
        let request_data = req.into_inner();

        let serialized = bincode::serialize(&request_data)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize request: {e}")))?;

        let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
        frame.put_u8(0);
        frame.put_u32(serialized.len() as u32);
        frame.extend_from_slice(&serialized);

        let uri = format!(
            "{}/infinisvm.sync.InfiniSVMService/InjectCommitBatchNotification",
            self.base_uri
        )
        .parse::<hyper::Uri>()
        .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

        let http_request = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::from(frame.freeze()))
            .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

        let mut response = self
            .http_client
            .request(http_request)
            .await
            .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

        if response.status() != hyper::StatusCode::OK {
            return Err(tonic::Status::internal(format!(
                "Unexpected status: {}",
                response.status()
            )));
        }

        if let Some(gs) = response.headers().get("grpc-status") {
            let code_i32 = gs.to_str().ok().and_then(|s| s.parse::<i32>().ok()).unwrap_or(2);
            if code_i32 != 0 {
                let msg = response
                    .headers()
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                return Err(tonic::Status::new(tonic::Code::from_i32(code_i32), msg));
            }
        }

        let mut body_bytes = bytes::BytesMut::new();
        while let Some(frame) = response.body_mut().frame().await {
            match frame {
                Ok(frame) => {
                    let bytes = match frame.into_data() {
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    body_bytes.extend_from_slice(&bytes);
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("Body read failed: {e}")));
                }
            }
        }

        let body_bytes = body_bytes.freeze();
        if body_bytes.len() < 5 {
            return Err(tonic::Status::internal("Response too short"));
        }
        let message_bytes = &body_bytes[5..];
        let decoded: InjectCommitBatchResponse = bincode::deserialize(message_bytes)
            .map_err(|e| tonic::Status::internal(format!("Failed to deserialize response: {e}")))?;

        Ok(tonic::Response::new(decoded))
    }
}

// Server module
pub mod infini_svm_service_server {
    pub use super::{InfiniSvmService, InfiniSvmServiceServer};
}

// Client module
pub mod infini_svm_service_client {
    pub use super::InfiniSvmServiceClient;
}

// Custom streaming type for bincode responses
pub struct BincodeStreaming<T> {
    inner: tokio_stream::wrappers::ReceiverStream<Result<T, tonic::Status>>,
}

impl<T> BincodeStreaming<T> {
    pub fn new(receiver: tokio::sync::mpsc::Receiver<Result<T, tonic::Status>>) -> Self {
        Self {
            inner: tokio_stream::wrappers::ReceiverStream::new(receiver),
        }
    }
}

impl<T> Stream for BincodeStreaming<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
