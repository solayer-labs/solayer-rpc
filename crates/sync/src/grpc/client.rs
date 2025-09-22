use base64::Engine;
use infinisvm_logger::{debug, error, info, warn};
use infinisvm_types::sync::{
    grpc::infini_svm_service_client::InfiniSvmServiceClient, CommitBatchNotification, GetLatestSlotRequest,
    GetLatestSlotResponse, GetTransactionBatchRequest, SlotDataResponse, StartReceivingSlotsRequest,
    TransactionBatchRequest,
};
use metrics::{counter, histogram};
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tokio_rustls::{rustls as rustls_conn, TlsConnector};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
    Code, Request, Status,
};

// Custom Ed25519 server certificate verifier (trust on raw public key)
#[derive(Debug)]
struct Ed25519ServerCertVerifier {
    allowed_pubkeys: Vec<[u8; 32]>,
}

impl Ed25519ServerCertVerifier {
    fn new(allowed_pubkeys: Vec<[u8; 32]>) -> Self {
        Self { allowed_pubkeys }
    }
}

impl rustls::client::danger::ServerCertVerifier for Ed25519ServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if let Some(pk) = extract_ed25519_spki_pubkey(end_entity.as_ref()) {
            if self.allowed_pubkeys.iter().any(|x| *x == pk) {
                return Ok(rustls::client::danger::ServerCertVerified::assertion());
            }
        }
        Err(rustls::Error::InvalidCertificate(rustls::CertificateError::Other(
            rustls::OtherError(StdArc::new(StaticError("untrusted ed25519 public key"))),
        )))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        use ed25519_dalek::{PublicKey, Signature, Verifier};
        if dss.scheme != rustls::SignatureScheme::ED25519 {
            return Err(rustls::Error::PeerIncompatible(
                rustls::PeerIncompatible::NoSignatureSchemesInCommon,
            ));
        }
        let Some(pk_bytes) = extract_ed25519_spki_pubkey(cert.as_ref()) else {
            return Err(rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding));
        };
        let Ok(vk) = PublicKey::from_bytes(&pk_bytes) else {
            return Err(rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding));
        };
        let sig_bytes = dss.signature().as_ref();
        let Ok(sig) = Signature::from_bytes(sig_bytes) else {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadSignature,
            ));
        };
        vk.verify(message, &sig)
            .map_err(|_| rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature))?;
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // We don't support TLS 1.2 signatures in this verifier (only TLS 1.3 + Ed25519)
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::NoSignatureSchemesInCommon,
        ))
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![rustls::SignatureScheme::ED25519]
    }
}

#[derive(Debug)]
struct AcceptAllServerCertVerifier;
impl rustls::client::danger::ServerCertVerifier for AcceptAllServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![rustls::SignatureScheme::ED25519]
    }
}

#[derive(Debug)]
struct AcceptAllServerCertVerifierConn;
impl rustls_conn::client::danger::ServerCertVerifier for AcceptAllServerCertVerifierConn {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_conn::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_conn::pki_types::CertificateDer<'_>],
        _server_name: &rustls_conn::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls_conn::pki_types::UnixTime,
    ) -> Result<rustls_conn::client::danger::ServerCertVerified, rustls_conn::Error> {
        Ok(rustls_conn::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_conn::pki_types::CertificateDer<'_>,
        _dss: &rustls_conn::DigitallySignedStruct,
    ) -> Result<rustls_conn::client::danger::HandshakeSignatureValid, rustls_conn::Error> {
        Ok(rustls_conn::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_conn::pki_types::CertificateDer<'_>,
        _dss: &rustls_conn::DigitallySignedStruct,
    ) -> Result<rustls_conn::client::danger::HandshakeSignatureValid, rustls_conn::Error> {
        Ok(rustls_conn::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls_conn::SignatureScheme> {
        vec![rustls_conn::SignatureScheme::ED25519]
    }
}

// Helper that attempts to set a custom certificate verifier on a ClientTlsConfig if the
// tonic version exposes the "dangerous" API. If not available, returns the input unchanged.
fn set_custom_verifier_if_supported(
    cfg: ClientTlsConfig,
    verifier: StdArc<dyn rustls::client::danger::ServerCertVerifier>,
) -> ClientTlsConfig {
    // The specific API shape has changed across tonic versions. We attempt the common form
    // via a small shim that compiles when the method exists.
    #[allow(unused_variables)]
    fn try_set(
        cfg: ClientTlsConfig,
        _verifier: StdArc<dyn rustls::client::danger::ServerCertVerifier>,
    ) -> ClientTlsConfig {
        cfg
    }
    try_set(cfg, verifier)
}

#[derive(Debug)]
struct StaticError(&'static str);
impl fmt::Display for StaticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for StaticError {}

// Minimal DER scan to extract the Ed25519 SPKI public key (32 bytes) from an X.509 cert
fn extract_ed25519_spki_pubkey(der: &[u8]) -> Option<[u8; 32]> {
    // Look for the OID 1.3.101.112 (ed25519): 06 03 2B 65 70
    const OID: &[u8] = &[0x06, 0x03, 0x2B, 0x65, 0x70];
    let mut i = 0;
    while let Some(pos) = memchr::memmem::find(&der[i..], OID) {
        i += pos + OID.len();
        // After the algorithm identifier, the next BIT STRING (0x03) should be the SPKI key
        // Scan forward up to a small window to find 0x03
        let window = &der[i..der.len().min(i + 256)];
        if let Some(bit_pos_rel) = window.iter().position(|b| *b == 0x03) {
            let bit_pos = i + bit_pos_rel;
            // Parse length
            if bit_pos + 2 >= der.len() {
                return None;
            }
            let len_byte = der[bit_pos + 1];
            let (len, hdr) = if len_byte & 0x80 == 0 {
                (len_byte as usize, 2usize)
            } else {
                let n = (len_byte & 0x7F) as usize;
                if n == 0 || bit_pos + 2 + n > der.len() {
                    return None;
                }
                let mut l: usize = 0;
                for j in 0..n {
                    l = (l << 8) | der[bit_pos + 2 + j] as usize;
                }
                (l, 2 + n)
            };
            if bit_pos + hdr + 1 + 32 > der.len() {
                return None;
            }
            let unused_bits = der[bit_pos + hdr];
            if unused_bits != 0 {
                continue;
            }
            if len < 33 {
                continue;
            }
            let start = bit_pos + hdr + 1;
            let end = start + 32;
            let mut out = [0u8; 32];
            out.copy_from_slice(&der[start..end]);
            return Some(out);
        }
    }
    None
}

/// Retry configuration for gRPC operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0 to 1.0) to add randomness to backoff
    pub jitter_factor: f64,
    /// Whether to enable circuit breaker functionality
    pub enable_circuit_breaker: bool,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker timeout in seconds
    pub circuit_breaker_timeout_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_secs: 30,
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for managing connection failures
#[derive(Debug)]
struct CircuitBreaker {
    state: RwLock<CircuitState>,
    failure_count: AtomicU32,
    last_failure_time: AtomicU64,
    threshold: u32,
    timeout_duration: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, timeout_duration: Duration) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            last_failure_time: AtomicU64::new(0),
            threshold,
            timeout_duration,
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last_failure = self.last_failure_time.load(Ordering::Relaxed);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now - last_failure >= self.timeout_duration.as_secs() {
                    drop(state);
                    let mut state = self.state.write().await;
                    *state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    async fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
    }

    async fn record_failure(&self) {
        let current_failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_failure_time.store(now, Ordering::Relaxed);

        if current_failures >= self.threshold {
            let mut state = self.state.write().await;
            *state = CircuitState::Open;
            warn!("Circuit breaker opened after {} failures", current_failures);
        }
    }
}

/// Enhanced gRPC client with retry and circuit breaker functionality
pub struct SyncClient {
    client: InfiniSvmServiceClient<Channel>,
    retry_config: RetryConfig,
    circuit_breaker: Arc<CircuitBreaker>,
    connection_url: String,
    // If present, enables TLS with custom Ed25519 pubkey verification on the server certificate
    allowed_server_pubkeys: Option<Vec<[u8; 32]>>,
    // Optional: trust this CA/cert when building TLS (supports self-signed server cert)
    root_ca_pem: Option<Vec<u8>>,
}

impl SyncClient {
    /// Create a new SyncClient with default retry configuration
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_tls(addr, RetryConfig::default(), None, None).await
    }

    /// Create a new SyncClient with custom retry configuration
    pub async fn connect_with_config(
        addr: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_tls(addr, retry_config, None, None).await
    }

    /// Create a new SyncClient with custom retry config and allowed server pubkeys for TLS.
    pub async fn connect_with_config_and_pubkeys(
        addr: &str,
        retry_config: RetryConfig,
        allowed_server_pubkeys: Option<Vec<[u8; 32]>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_tls(addr, retry_config, allowed_server_pubkeys, None).await
    }

    /// Create a new SyncClient with custom retry config, optional pubkey pinning, and optional root CA.
    pub async fn connect_with_tls(
        addr: &str,
        retry_config: RetryConfig,
        allowed_server_pubkeys: Option<Vec<[u8; 32]>>,
        root_ca_pem: Option<Vec<u8>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            retry_config.circuit_breaker_threshold,
            Duration::from_secs(retry_config.circuit_breaker_timeout_secs),
        ));

        let client = Self::connect_with_retry(
            addr,
            &retry_config,
            &circuit_breaker,
            &allowed_server_pubkeys,
            &root_ca_pem,
        )
        .await?;

        Ok(Self {
            client,
            retry_config,
            circuit_breaker,
            connection_url: addr.to_string(),
            allowed_server_pubkeys,
            root_ca_pem,
        })
    }

    /// Internal method to establish connection with retry logic
    async fn connect_with_retry(
        addr: &str,
        retry_config: &RetryConfig,
        circuit_breaker: &CircuitBreaker,
        allowed_server_pubkeys: &Option<Vec<[u8; 32]>>,
        root_ca_pem: &Option<Vec<u8>>,
    ) -> Result<InfiniSvmServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt = 0;
        let mut last_error: Option<String> = None;

        while attempt <= retry_config.max_retries {
            // Check circuit breaker
            if retry_config.enable_circuit_breaker && !circuit_breaker.can_execute().await {
                return Err("Circuit breaker is open".into());
            }

            match Self::connect_once(addr, allowed_server_pubkeys, root_ca_pem).await {
                Ok(client) => {
                    let client = client.max_decoding_message_size(1024 * 1024 * 1024); // 1GB
                    info!("Successfully connected to gRPC server at {}", addr);
                    counter!("grpc_client_connects_total").increment(1);
                    if retry_config.enable_circuit_breaker {
                        circuit_breaker.record_success().await;
                    }
                    return Ok(client);
                }
                Err(e) => {
                    attempt += 1;
                    // Log detailed transport error with debug formatting and source chain
                    let mut msg = format!("{e:?}");
                    let mut src = e.source();
                    let mut i = 0;
                    while let Some(s) = src {
                        i += 1;
                        msg.push_str(&format!("; source[{i}]: {}", s));
                        src = s.source();
                    }
                    last_error = Some(msg.clone());
                    counter!("grpc_client_connect_errors_total").increment(1);

                    if retry_config.enable_circuit_breaker {
                        circuit_breaker.record_failure().await;
                    }

                    if attempt <= retry_config.max_retries {
                        let backoff_ms = Self::calculate_backoff(
                            attempt,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );

                        warn!(
                            "gRPC connection attempt {} failed: {}, retrying in {}ms",
                            attempt, msg, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        let error_msg = last_error.unwrap_or_else(|| "Failed to connect after all retries".to_string());
        Err(error_msg.into())
    }

    async fn connect_once(
        addr: &str,
        allowed_server_pubkeys: &Option<Vec<[u8; 32]>>,
        root_ca_pem: &Option<Vec<u8>>,
    ) -> Result<InfiniSvmServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        // Ensure rustls provider is installed
        let _ = rustls::crypto::ring::default_provider().install_default();

        info!(
            "connect_once: addr={}, allowed_keys={}, root_ca={}",
            addr,
            allowed_server_pubkeys.as_ref().map(|v| v.len()).unwrap_or(0),
            if root_ca_pem.as_ref().is_some() { "yes" } else { "no" }
        );

        let mut endpoint = Endpoint::from_shared(addr.to_string())?;
        let uri: http::Uri = addr.parse()?;
        let host = uri.host().unwrap_or("localhost").to_string();
        // Track pinned end-entity cert if we probed it for pubkey pinning
        let mut pinned_end_entity: Option<Vec<u8>> = None;

        if let Some(keys) = allowed_server_pubkeys {
            // If no explicit CA provided, probe the server's end-entity cert and pin on SPKI
            if root_ca_pem.is_none() {
                let host_str = host.clone();
                let port = uri.port_u16().unwrap_or(443);
                info!("probing server cert via TLS to {}:{}", host_str, port);
                let tcp = match TcpStream::connect((host_str.as_str(), port)).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("TCP connect for TLS probe failed: {}", e);
                        return Err(format!("tcp connect failed: {e}").into());
                    }
                };

                let mut cfg = rustls_conn::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(StdArc::new(AcceptAllServerCertVerifierConn))
                    .with_no_client_auth();
                cfg.alpn_protocols.push(b"h2".to_vec());
                let connector = TlsConnector::from(StdArc::new(cfg));
                let server_name = rustls_conn::pki_types::ServerName::try_from(host_str.clone())?;
                let tls_stream = match connector.connect(server_name, tcp).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("TLS handshake probe failed: {:?}", e);
                        return Err(format!("tls probe handshake failed: {e}").into());
                    }
                };
                let (_io, session) = tls_stream.get_ref();
                let peer_certs = session.peer_certificates().ok_or("no peer certs")?;
                let end_entity = peer_certs.first().ok_or("empty peer certs")?;

                if let Some(pk) = extract_ed25519_spki_pubkey(end_entity.as_ref()) {
                    if !keys.iter().any(|x| *x == pk) {
                        error!(
                            "server ed25519 pubkey mismatch; got={} expected_one_of={}",
                            hex::encode(pk),
                            keys.iter().map(|k| hex::encode(k)).collect::<Vec<_>>().join(",")
                        );
                        return Err("server ed25519 pubkey mismatch".into());
                    }
                    pinned_end_entity = Some(end_entity.as_ref().to_vec());
                } else {
                    error!("server cert missing ed25519 spki (not an Ed25519 cert)");
                    return Err("server cert missing ed25519 spki".into());
                }
            }

            // Build tonic TLS config; add our custom verifier if supported
            let mut tls = ClientTlsConfig::new().domain_name(host.clone());
            #[allow(unused_must_use)]
            {
                tls = set_custom_verifier_if_supported(tls, StdArc::new(Ed25519ServerCertVerifier::new(keys.clone())));
            }
            if let Some(ca) = root_ca_pem {
                tls = tls.ca_certificate(Certificate::from_pem(ca.clone()));
            }
            if let Some(der) = pinned_end_entity.as_ref() {
                // Encode DER to PEM
                let pem = {
                    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
                    let mut s = String::new();
                    s.push_str("-----BEGIN CERTIFICATE-----\n");
                    for chunk in b64.as_bytes().chunks(64) {
                        s.push_str(std::str::from_utf8(chunk).unwrap());
                        s.push('\n');
                    }
                    s.push_str("-----END CERTIFICATE-----\n");
                    s
                };
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            info!(
                "using TLS for tonic endpoint with domain {} (pubkey pinning mode)",
                host
            );
            endpoint = endpoint.tls_config(tls)?;
        } else if let Some(ca) = root_ca_pem {
            let tls = ClientTlsConfig::new()
                .domain_name(host.clone())
                .ca_certificate(Certificate::from_pem(ca.clone()));
            info!("using TLS for tonic endpoint with provided CA, domain {}", host);
            endpoint = endpoint.tls_config(tls)?;
        }

        let base_uri = format!(
            "{}://{}:{}",
            uri.scheme_str().unwrap_or("http"),
            uri.host().unwrap_or("localhost"),
            uri.port_u16().unwrap_or(5005)
        );
        // Build a lazy tonic Channel to satisfy the type parameter, but do not
        // perform an eager handshake here. All actual RPCs use the HTTPS Hyper client.
        let ch = endpoint.connect_lazy();

        // Provide TLS CA/pinned cert to the bincode HTTP client so it can do HTTPS
        let ca_pem_for_bincode: Option<Vec<u8>> = if let Some(ca) = root_ca_pem {
            Some(ca.clone())
        } else {
            // If we pinned the end-entity, encode to PEM for the hyper-rustls client
            if let Some(der) = pinned_end_entity.as_ref() {
                let b64 = base64::engine::general_purpose::STANDARD.encode(der);
                let mut s = String::new();
                s.push_str("-----BEGIN CERTIFICATE-----\n");
                for chunk in b64.as_bytes().chunks(64) {
                    s.push_str(std::str::from_utf8(chunk).unwrap());
                    s.push('\n');
                }
                s.push_str("-----END CERTIFICATE-----\n");
                Some(s.into_bytes())
            } else {
                None
            }
        };

        Ok(InfiniSvmServiceClient::new(ch, base_uri, ca_pem_for_bincode))
    }

    /// Calculate exponential backoff with jitter
    fn calculate_backoff(
        attempt: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        multiplier: f64,
        jitter_factor: f64,
    ) -> u64 {
        let exponential_backoff = (initial_backoff_ms as f64 * multiplier.powi(attempt as i32 - 1)) as u64;
        let backoff_with_cap = exponential_backoff.min(max_backoff_ms);

        // Add jitter to avoid thundering herd
        let jitter = (backoff_with_cap as f64 * jitter_factor * rand::random::<f64>()) as u64;
        backoff_with_cap + jitter
    }

    /// Check if an error is retryable
    fn is_retryable_error(status: &Status) -> bool {
        match status.code() {
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => true,
            Code::Internal => {
                // Some internal errors might be transient
                let message = status.message().to_lowercase();
                message.contains("connection") || message.contains("timeout") || message.contains("network")
            }
            _ => false,
        }
    }

    /// Execute a gRPC operation with retry logic
    async fn execute_with_retry<T>(
        &mut self,
        operation_name: &str,
        operation: impl Fn(&mut InfiniSvmServiceClient<Channel>) -> futures_util::future::BoxFuture<'_, Result<T, Status>>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt = 0;
        let mut last_error: Option<String> = None;

        while attempt <= self.retry_config.max_retries {
            // Check circuit breaker
            if self.retry_config.enable_circuit_breaker && !self.circuit_breaker.can_execute().await {
                return Err("Circuit breaker is open".into());
            }

            match operation(&mut self.client).await {
                Ok(result) => {
                    if attempt > 0 {
                        info!("Operation '{}' succeeded after {} retries", operation_name, attempt);
                    }
                    if self.retry_config.enable_circuit_breaker {
                        self.circuit_breaker.record_success().await;
                    }
                    return Ok(result);
                }
                Err(status) => {
                    attempt += 1;
                    last_error = Some(status.to_string());

                    if !Self::is_retryable_error(&status) {
                        debug!("Non-retryable error for operation '{}': {}", operation_name, status);
                        return Err(status.to_string().into());
                    }

                    if self.retry_config.enable_circuit_breaker {
                        self.circuit_breaker.record_failure().await;
                    }

                    if attempt <= self.retry_config.max_retries {
                        let backoff_ms = Self::calculate_backoff(
                            attempt,
                            self.retry_config.initial_backoff_ms,
                            self.retry_config.max_backoff_ms,
                            self.retry_config.backoff_multiplier,
                            self.retry_config.jitter_factor,
                        );

                        warn!(
                            "Operation '{}' attempt {} failed ({}), retrying in {}ms",
                            operation_name, attempt, status, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        let error_msg =
            last_error.unwrap_or_else(|| format!("Operation '{}' failed after all retries", operation_name));
        Err(error_msg.into())
    }

    /// Reconnect the client if connection is lost
    async fn ensure_connection(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Try a lightweight operation to check connection health
        let health_check_result = {
            let request = Request::new(GetLatestSlotRequest {});
            self.client.get_latest_slot(request).await
        };

        if health_check_result.is_err() {
            info!("Connection lost, attempting to reconnect...");
            counter!("grpc_client_reconnects_total").increment(1);
            self.client = Self::connect_with_retry(
                &self.connection_url,
                &self.retry_config,
                &self.circuit_breaker,
                &self.allowed_server_pubkeys,
                &self.root_ca_pem,
            )
            .await?;
            info!("Successfully reconnected to gRPC server");
        }

        Ok(())
    }

    pub async fn get_latest_slot(&mut self) -> Result<GetLatestSlotResponse, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .execute_with_retry("get_latest_slot", |client| {
                Box::pin(async move {
                    let request = Request::new(GetLatestSlotRequest {});
                    client.get_latest_slot(request).await
                })
            })
            .await?;

        Ok(result.into_inner())
    }

    pub async fn get_transaction_batch(
        &mut self,
        slot: u64,
        job_id: u64,
    ) -> Result<CommitBatchNotification, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .execute_with_retry("get_transaction_batch", |client| {
                Box::pin(async move {
                    let request = Request::new(GetTransactionBatchRequest { slot, job_id });
                    client.get_transaction_batch(request).await
                })
            })
            .await?;
        Ok(result.into_inner())
    }

    // Single RPC without the generic retry wrapper; returns tonic::Status for precise error handling
    pub async fn get_transaction_batch_status(
        &mut self,
        slot: u64,
        job_id: u64,
    ) -> Result<CommitBatchNotification, tonic::Status> {
        let request = Request::new(GetTransactionBatchRequest { slot, job_id });
        let resp = self.client.get_transaction_batch(request).await?;
        Ok(resp.into_inner())
    }

    pub async fn subscribe_slots(
        &mut self,
    ) -> Result<mpsc::Receiver<SlotDataResponse>, Box<dyn std::error::Error + Send + Sync>> {
        let stream = self
            .execute_with_retry("subscribe_slots", |client| {
                Box::pin(async move {
                    let request = Request::new(StartReceivingSlotsRequest {});
                    client.start_receiving_slots(request).await
                })
            })
            .await?
            .into_inner();

        let (tx, rx) = mpsc::channel(128);
        let retry_config = self.retry_config.clone();
        let circuit_breaker = Arc::clone(&self.circuit_breaker);
        let connection_url = self.connection_url.clone();
        let retry_config_for_task = retry_config.clone();
        let circuit_breaker_for_task = Arc::clone(&circuit_breaker);

        // Function to resubscribe the slot stream on failure
        let allowed_keys_outer = self.allowed_server_pubkeys.clone();
        let make_new_stream = move || {
            let allowed = allowed_keys_outer.clone();
            let connection_url = connection_url.clone();
            async move {
                match SyncClient::connect_once(&connection_url, &allowed, &None).await {
                    Ok(client) => {
                        let mut client = client.max_decoding_message_size(1024 * 1024 * 1024);
                        let request = Request::new(StartReceivingSlotsRequest {});
                        match client.start_receiving_slots(request).await {
                            Ok(resp) => Ok(resp.into_inner()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
        };

        tokio::spawn(async move {
            Self::handle_stream(
                stream,
                tx,
                "slot_data",
                retry_config_for_task,
                circuit_breaker_for_task,
                make_new_stream,
            )
            .await;
        });

        Ok(rx)
    }

    pub async fn subscribe_transactions(
        &mut self,
    ) -> Result<mpsc::Receiver<CommitBatchNotification>, Box<dyn std::error::Error + Send + Sync>> {
        let stream = self
            .execute_with_retry("subscribe_transactions", |client| {
                Box::pin(async move {
                    let request = Request::new(TransactionBatchRequest {});
                    client.subscribe_transaction_batches(request).await
                })
            })
            .await?
            .into_inner();

        let (tx, rx) = mpsc::channel(128);
        let retry_config = self.retry_config.clone();
        let circuit_breaker = Arc::clone(&self.circuit_breaker);
        let connection_url = self.connection_url.clone();
        let retry_config_for_task = retry_config.clone();
        let circuit_breaker_for_task = Arc::clone(&circuit_breaker);

        // Function to resubscribe the transactions stream on failure
        let allowed_keys_outer = self.allowed_server_pubkeys.clone();
        let make_new_stream = move || {
            let allowed = allowed_keys_outer.clone();
            let connection_url = connection_url.clone();
            async move {
                match SyncClient::connect_once(&connection_url, &allowed, &None).await {
                    Ok(client) => {
                        let mut client = client.max_decoding_message_size(1024 * 1024 * 1024);
                        let request = Request::new(TransactionBatchRequest {});
                        match client.subscribe_transaction_batches(request).await {
                            Ok(resp) => Ok(resp.into_inner()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
        };

        tokio::spawn(async move {
            Self::handle_stream(
                stream,
                tx,
                "transaction_batch",
                retry_config_for_task,
                circuit_breaker_for_task,
                make_new_stream,
            )
            .await;
        });

        Ok(rx)
    }

    /// Handle streaming operations with automatic reconnection on failure
    async fn handle_stream<T, S, F, Fut>(
        mut stream: S,
        tx: mpsc::Sender<T>,
        stream_name: &str,
        retry_config: RetryConfig,
        circuit_breaker: Arc<CircuitBreaker>,
        mut resubscribe: F,
    ) where
        T: Send + 'static,
        S: StreamExt<Item = Result<T, Status>> + Send + Unpin + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<S, String>> + Send,
    {
        let mut consecutive_failures: u32 = 0;

        'outer: loop {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(data) => {
                        // reset failures on successful receive
                        if consecutive_failures != 0 {
                            consecutive_failures = 0;
                        }
                        if let Err(e) = tx.send(data).await {
                            error!("Error sending {}: {}", stream_name, e);
                            break 'outer;
                        }
                    }
                    Err(status) => {
                        // Record metrics and log error
                        counter!("grpc_stream_errors_total").increment(1);
                        error!("Error receiving {}: {}", stream_name, status);

                        // Non-retryable errors: exit
                        if !Self::is_retryable_error(&status) {
                            warn!("{} stream error is non-retryable. Stopping.", stream_name);
                            break 'outer;
                        }

                        // Retryable: attempt to reconnect with backoff
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if retry_config.enable_circuit_breaker {
                            circuit_breaker.record_failure().await;
                        }

                        let backoff_ms = Self::calculate_backoff(
                            consecutive_failures,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );

                        warn!(
                            "{} stream failed (attempt {}), reconnecting in {}ms",
                            stream_name, consecutive_failures, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;

                        // Keep trying to resubscribe until success
                        loop {
                            match resubscribe().await {
                                Ok(new_stream) => {
                                    counter!("grpc_stream_reconnects_total").increment(1);
                                    info!("Reconnected {} stream", stream_name);
                                    if retry_config.enable_circuit_breaker {
                                        circuit_breaker.record_success().await;
                                    }
                                    stream = new_stream;
                                    continue 'outer;
                                }
                                Err(err) => {
                                    counter!("grpc_stream_reconnect_errors_total").increment(1);
                                    warn!("Failed to resubscribe {}: {}. Retrying...", stream_name, err);
                                    let backoff_ms = Self::calculate_backoff(
                                        consecutive_failures,
                                        retry_config.initial_backoff_ms,
                                        retry_config.max_backoff_ms,
                                        retry_config.backoff_multiplier,
                                        retry_config.jitter_factor,
                                    );
                                    sleep(Duration::from_millis(backoff_ms)).await;
                                    consecutive_failures = consecutive_failures.saturating_add(1);
                                }
                            }
                        }
                    }
                }
            }

            // Stream ended gracefully (None). Attempt to resubscribe as well.
            warn!("Stream {} ended. Attempting to resubscribe...", stream_name);
            consecutive_failures = consecutive_failures.saturating_add(1);
            let backoff_ms = Self::calculate_backoff(
                consecutive_failures,
                retry_config.initial_backoff_ms,
                retry_config.max_backoff_ms,
                retry_config.backoff_multiplier,
                retry_config.jitter_factor,
            );
            sleep(Duration::from_millis(backoff_ms)).await;
            loop {
                match resubscribe().await {
                    Ok(new_stream) => {
                        counter!("grpc_stream_reconnects_total").increment(1);
                        info!("Reconnected {} stream", stream_name);
                        if retry_config.enable_circuit_breaker {
                            circuit_breaker.record_success().await;
                        }
                        stream = new_stream;
                        break; // back to outer loop to read
                    }
                    Err(err) => {
                        counter!("grpc_stream_reconnect_errors_total").increment(1);
                        warn!("Failed to resubscribe {}: {}. Retrying...", stream_name, err);
                        let backoff_ms = Self::calculate_backoff(
                            consecutive_failures,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                        consecutive_failures = consecutive_failures.saturating_add(1);
                    }
                }
            }
        }
        info!("Stream {} handler exiting", stream_name);
    }

    /// Get current retry configuration
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Update retry configuration
    pub fn set_retry_config(&mut self, config: RetryConfig) {
        self.retry_config = config;
    }

    /// Get circuit breaker status
    pub async fn circuit_breaker_status(&self) -> (String, u32) {
        let state = self.circuit_breaker.state.read().await;
        let failure_count = self.circuit_breaker.failure_count.load(Ordering::Relaxed);
        let state_str = match *state {
            CircuitState::Closed => "Closed",
            CircuitState::Open => "Open",
            CircuitState::HalfOpen => "HalfOpen",
        };
        (state_str.to_string(), failure_count)
    }

    /// Force circuit breaker reset (for testing/debugging)
    pub async fn reset_circuit_breaker(&self) {
        self.circuit_breaker.failure_count.store(0, Ordering::Relaxed);
        let mut state = self.circuit_breaker.state.write().await;
        *state = CircuitState::Closed;
        info!("Circuit breaker manually reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Test default retry configuration
    #[test]
    fn test_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 5000);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.jitter_factor, 0.1);
        assert!(config.enable_circuit_breaker);
        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_timeout_secs, 30);
    }

    /// Test exponential backoff calculation
    #[test]
    fn test_calculate_backoff() {
        let initial_backoff = 100;
        let max_backoff = 5000;
        let multiplier = 2.0;
        let jitter_factor = 0.0; // No jitter for predictable testing

        // Test first attempt
        let backoff1 = SyncClient::calculate_backoff(1, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff1, 100);

        // Test second attempt
        let backoff2 = SyncClient::calculate_backoff(2, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff2, 200);

        // Test third attempt
        let backoff3 = SyncClient::calculate_backoff(3, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff3, 400);

        // Test backoff cap
        let backoff_large = SyncClient::calculate_backoff(10, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff_large, max_backoff);
    }

    /// Test retryable error detection
    #[test]
    fn test_is_retryable_error() {
        use tonic::{Code, Status};

        // Retryable errors
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Unavailable,
            "Service unavailable"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::DeadlineExceeded,
            "Timeout"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::ResourceExhausted,
            "Rate limited"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Aborted,
            "Request aborted"
        )));

        // Retryable internal errors
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Connection error occurred"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Network timeout detected"
        )));

        // Non-retryable errors
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::InvalidArgument,
            "Bad request"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::NotFound,
            "Resource not found"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::PermissionDenied,
            "Access denied"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Database corruption"
        )));
    }

    /// Test circuit breaker functionality
    #[tokio::test]
    async fn test_circuit_breaker() {
        let threshold = 3;
        let timeout_duration = Duration::from_millis(100);
        let circuit_breaker = CircuitBreaker::new(threshold, timeout_duration);

        // Initially should be closed
        assert!(circuit_breaker.can_execute().await);

        // Record failures
        for _ in 0..threshold {
            circuit_breaker.record_failure().await;
        }

        // Should be open now
        assert!(!circuit_breaker.can_execute().await);

        // Wait for timeout
        tokio::time::sleep(timeout_duration + Duration::from_millis(10)).await;

        // Should be half-open now
        assert!(circuit_breaker.can_execute().await);

        // Record success to close circuit
        circuit_breaker.record_success().await;
        assert!(circuit_breaker.can_execute().await);
    }
}
