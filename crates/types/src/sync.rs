use serde::{Deserialize, Serialize};

// Bincode-based gRPC message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartReceivingSlotsRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotDataResponse {
    pub slot: u64,
    pub blockhash: Vec<u8>,
    pub parent_blockhash: Vec<u8>,
    pub timestamp: u64,
    pub job_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLatestSlotRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLatestSlotResponse {
    pub slot: u64,
    pub hash: Vec<u8>,
    pub parent_blockhash: Vec<u8>,
    pub timestamp: u64,
    pub job_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionBatchRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTransactionBatchRequest {
    pub slot: u64,
    pub job_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub signature: Vec<u8>,
    pub slot: u64,
    pub timestamp: u64,
    pub success: bool,
    pub error_message: Option<String>,
    pub accounts_involved: Vec<u8>,
    pub fee: u64,
    pub compute_units_consumed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitBatchNotification {
    pub slot: u64,
    pub timestamp: u64,
    pub batch_size: u32,
    pub compressed_transactions: Vec<u8>,
    pub compression_ratio: u64,
    pub job_id: u64,
    pub worker_id: usize,
    pub is_final: bool,
}

// Simple gRPC service definitions for bincode
pub mod grpc {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Instant,
    };

    use bytes::{Buf, BufMut};
    use futures_core::Stream;
    use http_body::Frame;
    use metrics::{counter, histogram};
    use tonic::transport::Channel;

    pub use super::*;

    // Service trait
    #[tonic::async_trait]
    pub trait InfiniSvmService: Send + Sync + 'static + Clone {
        type StartReceivingSlotsStream: Stream<Item = Result<SlotDataResponse, tonic::Status>> + Send + 'static;
        type SubscribeTransactionBatchesStream: Stream<Item = Result<CommitBatchNotification, tonic::Status>>
            + Send
            + 'static;

        async fn start_receiving_slots(
            &self,
            request: tonic::Request<StartReceivingSlotsRequest>,
        ) -> Result<tonic::Response<Self::StartReceivingSlotsStream>, tonic::Status>;

        async fn get_latest_slot(
            &self,
            request: tonic::Request<GetLatestSlotRequest>,
        ) -> Result<tonic::Response<GetLatestSlotResponse>, tonic::Status>;

        async fn subscribe_transaction_batches(
            &self,
            request: tonic::Request<TransactionBatchRequest>,
        ) -> Result<tonic::Response<Self::SubscribeTransactionBatchesStream>, tonic::Status>;

        async fn get_transaction_batch(
            &self,
            request: tonic::Request<GetTransactionBatchRequest>,
        ) -> Result<tonic::Response<CommitBatchNotification>, tonic::Status>;
    }

    // Server wrapper
    #[derive(Clone)]
    pub struct InfiniSvmServiceServer<T> {
        inner: T,
    }

    impl<T> InfiniSvmServiceServer<T>
    where
        T: InfiniSvmService,
    {
        pub fn new(inner: T) -> Self {
            Self { inner }
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
            let (parts, body) = req.into_parts();

            Box::pin(async move {
                let result: Result<
                    http::Response<http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, tonic::Status>>,
                    tonic::Status,
                > = async move {
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
                        "StartReceivingSlots" => {
                            // Deserialize request
                            let request: StartReceivingSlotsRequest =
                                bincode::deserialize(message_bytes).map_err(|e| {
                                    counter!("grpc_server_errors_total", "method" => "StartReceivingSlots", "kind" => "deserialize").increment(1);
                                    tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                                })?;

                            // Call the service method
                            let response = inner.start_receiving_slots(tonic::Request::new(request)).await?;
                            let stream = response.into_inner();

            // For streaming responses, directly map the stream into framed bytes to avoid extra hops
            let body_stream = tokio_stream::StreamExt::map(stream, |item_res: Result<SlotDataResponse, tonic::Status>| {
                match item_res {
                    Ok(item) => match bincode::serialize(&item) {
                        Ok(serialized) => {
                            let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                            frame.put_u8(0); // No compression
                            frame.put_u32(serialized.len() as u32);
                            frame.extend_from_slice(&serialized);
                            counter!("grpc_server_messages_total", "method" => "StartReceivingSlots").increment(1);
                            counter!("grpc_server_bytes_total", "direction" => "tx", "method" => "StartReceivingSlots")
                                .increment(serialized.len() as u64);
                            Ok(Frame::data(frame.freeze()))
                        }
                        Err(e) => {
                            counter!("grpc_server_errors_total", "method" => "StartReceivingSlots", "kind" => "serialize").increment(1);
                            Err(tonic::Status::internal(format!(
                                "Failed to serialize response: {e}"
                            )))
                        }
                    },
                    Err(status) => Err(status),
                }
            });

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
                        "GetLatestSlot" => {
                            // Deserialize request
                            let request: GetLatestSlotRequest = bincode::deserialize(message_bytes).map_err(|e| {
                                counter!("grpc_server_errors_total", "method" => "GetLatestSlot", "kind" => "deserialize").increment(1);
                                tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                            })?;

                            // Call the service method
                            let start = Instant::now();
                            let response = inner.get_latest_slot(tonic::Request::new(request)).await?;
                            let response_data = response.into_inner();

                            // Serialize response
                            let serialized = bincode::serialize(&response_data)
                                .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {e}")))?;

                            // Create response with gRPC message framing
                            let mut frame = bytes::BytesMut::with_capacity(5 + serialized.len());
                            frame.put_u8(0); // No compression
                            frame.put_u32(serialized.len() as u32);
                            frame.extend_from_slice(&serialized);

                            // Metrics: server unary bytes/messages + latency
                            counter!("grpc_server_messages_total", "method" => "GetLatestSlot").increment(1);
                            counter!("grpc_server_bytes_total", "direction" => "tx", "method" => "GetLatestSlot")
                                .increment(serialized.len() as u64);
                            histogram!("grpc_server_unary_latency_ms", "method" => "GetLatestSlot")
                                .record(start.elapsed().as_secs_f64() * 1000.0);

                            let body = http_body_util::Full::new(frame.freeze())
                                .map_err(|_: std::convert::Infallible| tonic::Status::internal("impossible error"));
                            let boxed_body = http_body_util::combinators::UnsyncBoxBody::new(body);

                            let response = http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "0")
                                .header("grpc-accept-encoding", "identity,gzip")
                                .body(boxed_body)
                                .unwrap();
                            Ok(response)
                        }
                        "SubscribeTransactionBatches" => {
                            // Deserialize request
                            let request: TransactionBatchRequest =
                                bincode::deserialize(message_bytes).map_err(|e| {
                                    counter!("grpc_server_errors_total", "method" => "SubscribeTransactionBatches", "kind" => "deserialize").increment(1);
                                    tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                                })?;

                            // Call the service method
                            let response = inner
                                .subscribe_transaction_batches(tonic::Request::new(request))
                                .await?;
                            let stream = response.into_inner();

            // For streaming responses, directly map the stream into framed bytes to avoid extra hops
            let body_stream = tokio_stream::StreamExt::map(
                stream,
                |item_res: Result<CommitBatchNotification, tonic::Status>| match item_res {
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
                            let request: GetTransactionBatchRequest = bincode::deserialize(message_bytes).map_err(|e| {
                                tonic::Status::internal(format!("Failed to deserialize request: {e}"))
                            })?;

                            // Call the service method
                            let response = inner.get_transaction_batch(tonic::Request::new(request)).await?;
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
                        counter!("grpc_server_errors_total", "kind" => "handler", "code" => format!("{}", status.code())).increment(1);
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
    #[allow(dead_code)]
    pub struct InfiniSvmServiceClient<T> {
        inner: T,
        // HTTPS-capable Hyper client (also supports plain HTTP)
        http_client: hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>,
        base_uri: String,
    }

    impl InfiniSvmServiceClient<Channel> {
        pub fn new(channel: Channel, base_uri: String, tls_ca_pem: Option<Vec<u8>>) -> Self {
            // Build an HTTPS-capable connector (also supports HTTP/h2c)
            let https = match tls_ca_pem {
                Some(ca_pem) => {
                    // Build a rustls ClientConfig with the provided CA (or pinned end-entity)
                    let mut root_store = rustls::RootCertStore::empty();
                    {
                        use std::io::BufReader;
                        let mut reader = BufReader::new(&ca_pem[..]);
                        if let Ok(certs) = rustls_pemfile::certs(&mut reader) {
                            for cert in certs {
                                let _ = root_store.add(&rustls::Certificate(cert));
                            }
                        } else {
                            let _ = root_store.add(&rustls::Certificate(ca_pem));
                        }
                    }
                    let tls_config = rustls::ClientConfig::builder()
                        .with_safe_defaults()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_tls_config(tls_config)
                        .https_or_http()
                        .enable_http2()
                        .build()
                }
                None => {
                    // Use system/webpki roots
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_webpki_roots()
                        .https_or_http()
                        .enable_http2()
                        .build()
                }
            };

            let http_client = hyper::Client::builder()
                .http2_only(true)
                .http2_keep_alive_interval(std::time::Duration::from_secs(10))
                .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
                .build::<_, hyper::Body>(https);

            Self {
                inner: channel,
                http_client,
                base_uri,
            }
        }

        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<tonic::codegen::StdError>,
        {
            let endpoint = tonic::transport::Endpoint::new(dst)?;
            let uri = endpoint.uri().clone();
            let conn = endpoint.connect().await?;
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http2()
                .build();
            let http_client = hyper::Client::builder()
                .http2_only(true)
                .http2_keep_alive_interval(std::time::Duration::from_secs(10))
                .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
                .build::<_, hyper::Body>(https);

            // Extract base URI from the endpoint
            let base_uri = format!(
                "{}://{}:{}",
                uri.scheme_str().unwrap_or("http"),
                uri.host().unwrap_or("localhost"),
                uri.port_u16().unwrap_or(5005)
            );

            Ok(Self {
                inner: conn,
                http_client,
                base_uri,
            })
        }

        pub fn max_decoding_message_size(self, _limit: usize) -> Self {
            self
        }

        pub fn max_encoding_message_size(self, _limit: usize) -> Self {
            self
        }

        // Bincode-based gRPC client methods
        pub async fn start_receiving_slots(
            &mut self,
            request: impl tonic::IntoRequest<StartReceivingSlotsRequest>,
        ) -> Result<tonic::Response<BincodeStreaming<SlotDataResponse>>, tonic::Status> {
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
            let uri = format!("{}/infinisvm.sync.InfiniSVMService/StartReceivingSlots", self.base_uri)
                .parse::<hyper::Uri>()
                .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

            let http_request = hyper::Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .body(hyper::Body::from(frame.freeze()))
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
            let mut body = response.into_body();

            // Spawn a task to read the streaming response
            tokio::spawn(async move {
                use futures_util::StreamExt;

                let mut buffer = bytes::BytesMut::new();

                while let Some(chunk) = body.next().await {
                    match chunk {
                        Ok(bytes) => {
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
                                    match bincode::deserialize::<SlotDataResponse>(message_bytes) {
                                        Ok(response) => {
                                            if tx.send(Ok(response)).await.is_err() {
                                                // Receiver dropped
                                                return;
                                            }
                                            counter!("grpc_client_messages_total", "method" => "StartReceivingSlots")
                                                .increment(1);
                                            counter!("grpc_client_bytes_total", "direction" => "rx", "method" => "StartReceivingSlots").increment(length as u64);
                                        }
                                        Err(e) => {
                                            counter!("grpc_client_errors_total", "method" => "StartReceivingSlots", "kind" => "deserialize").increment(1);
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
                            counter!("grpc_client_errors_total", "method" => "StartReceivingSlots", "kind" => "http_body").increment(1);
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

        pub async fn get_latest_slot(
            &mut self,
            request: impl tonic::IntoRequest<GetLatestSlotRequest>,
        ) -> Result<tonic::Response<GetLatestSlotResponse>, tonic::Status> {
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

            // Create HTTP request with hyper types
            let uri = format!("{}/infinisvm.sync.InfiniSVMService/GetLatestSlot", self.base_uri)
                .parse::<hyper::Uri>()
                .map_err(|e| tonic::Status::internal(format!("Failed to parse URI: {e}")))?;

            let http_request = hyper::Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/grpc")
                .body(hyper::Body::from(frame.freeze()))
                .map_err(|e| tonic::Status::internal(format!("Failed to build HTTP request: {e}")))?;

            // Use the shared HTTP client
            let response = self
                .http_client
                .request(http_request)
                .await
                .map_err(|e| tonic::Status::internal(format!("HTTP request failed: {e}")))?;

            // Read response body
            let start = Instant::now();
            let body_bytes = hyper::body::to_bytes(response.into_body())
                .await
                .map_err(|e| tonic::Status::internal(format!("Failed to read response body: {e}")))?;

            // Skip the gRPC message framing (5 bytes: 1 byte compression flag + 4 bytes
            // length)
            let message_bytes = if body_bytes.len() > 5 {
                &body_bytes[5..]
            } else {
                return Err(tonic::Status::internal("Response too short"));
            };

            // Deserialize response with bincode
            let response_data: GetLatestSlotResponse = bincode::deserialize(message_bytes)
                .map_err(|e| tonic::Status::internal(format!("Failed to deserialize response: {e}")))?;

            // Metrics: client unary bytes/messages and latency
            counter!("grpc_client_messages_total", "method" => "GetLatestSlot").increment(1);
            counter!("grpc_client_bytes_total", "direction" => "rx", "method" => "GetLatestSlot")
                .increment(message_bytes.len() as u64);
            histogram!("grpc_client_unary_latency_ms", "method" => "GetLatestSlot")
                .record(start.elapsed().as_secs_f64() * 1000.0);

            Ok(tonic::Response::new(response_data))
        }

        pub async fn subscribe_transaction_batches(
            &mut self,
            request: impl tonic::IntoRequest<TransactionBatchRequest>,
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
                .body(hyper::Body::from(frame.freeze()))
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
            let mut body = response.into_body();

            // Spawn a task to read the streaming response
            tokio::spawn(async move {
                use futures_util::StreamExt;

                let mut buffer = bytes::BytesMut::new();

                while let Some(chunk) = body.next().await {
                    match chunk {
                        Ok(bytes) => {
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
                                    let decoded = bincode::deserialize::<CommitBatchNotification>(message_bytes)
                                        .or_else(|_e| {
                                            #[derive(serde::Deserialize)]
                                            struct LegacyCommitBatchNotification {
                                                slot: u64,
                                                timestamp: u64,
                                                batch_size: u32,
                                                worker_id: usize,
                                                compressed_transactions: Vec<u8>,
                                                compression_ratio: u64,
                                            }
                                            bincode::deserialize::<LegacyCommitBatchNotification>(message_bytes).map(
                                                |legacy| CommitBatchNotification {
                                                    slot: legacy.slot,
                                                    timestamp: legacy.timestamp,
                                                    batch_size: legacy.batch_size,
                                                    compressed_transactions: legacy.compressed_transactions,
                                                    compression_ratio: legacy.compression_ratio,
                                                    job_id: 0,
                                                    worker_id: legacy.worker_id,
                                                    is_final: false,
                                                },
                                            )
                                        });

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

        pub async fn get_transaction_batch(
            &mut self,
            request: impl tonic::IntoRequest<GetTransactionBatchRequest>,
        ) -> Result<tonic::Response<CommitBatchNotification>, tonic::Status> {
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
                .body(hyper::Body::from(frame.freeze()))
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
            use futures_util::stream::StreamExt as _;
            let mut body_bytes = bytes::BytesMut::new();
            while let Some(chunk) = response.body_mut().next().await {
                let chunk = chunk.map_err(|e| tonic::Status::internal(format!("Body read failed: {e}")))?;
                body_bytes.extend_from_slice(&chunk);
            }

            let body_bytes = body_bytes.freeze();
            if body_bytes.len() < 5 {
                // If grpc-status header indicated success but body is empty, treat as internal
                // error
                return Err(tonic::Status::internal("Response too short"));
            }
            let message_bytes = &body_bytes[5..];
            // Try new format first, then legacy fallback
            let decoded: CommitBatchNotification = bincode::deserialize(message_bytes)
                .or_else(|_e| {
                    #[derive(serde::Deserialize)]
                    struct LegacyCommitBatchNotification {
                        slot: u64,
                        timestamp: u64,
                        batch_size: u32,
                        worker_id: usize,
                        compressed_transactions: Vec<u8>,
                        compression_ratio: u64,
                    }
                    bincode::deserialize::<LegacyCommitBatchNotification>(message_bytes).map(|legacy| {
                        CommitBatchNotification {
                            slot: legacy.slot,
                            timestamp: legacy.timestamp,
                            batch_size: legacy.batch_size,
                            compressed_transactions: legacy.compressed_transactions,
                            compression_ratio: legacy.compression_ratio,
                            job_id: 0,
                            worker_id: legacy.worker_id,
                            is_final: false,
                        }
                    })
                })
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
}
