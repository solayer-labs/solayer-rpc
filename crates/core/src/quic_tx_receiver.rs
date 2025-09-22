use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Sender;
use infinisvm_logger::{debug, error};
use quinn::{Connection, VarInt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use solana_sdk::{
    message::{v0::LoadedAddresses, SimpleAddressLoader},
    transaction::{MessageHash, SanitizedTransaction, VersionedTransaction},
};

use crate::metrics::QuicTxReceiverMetrics;

pub struct QuicTxReceiver {
    pub server: JoinHandle<()>,
    pub metrics: Arc<QuicTxReceiverMetrics>,
    pub reporting_thread: JoinHandle<()>,
}

async fn handle_connection(
    connection: Connection,
    sender: Sender<(SanitizedTransaction, u64)>,
    exit: Arc<AtomicBool>,
    metrics: Arc<QuicTxReceiverMetrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut send_stream, mut recv_stream) = connection.accept_bi().await?;

    const MAX_PAYLOAD_SIZE: usize = 2048; // Max expected payload size for a single transaction
    let mut receive_buffer = Vec::with_capacity(MAX_PAYLOAD_SIZE * 2); // Buffer for accumulating stream data
    let mut temp_read_buf = [0u8; 2048]; // Temporary buffer for each read operation

    while !exit.load(Ordering::Relaxed) {
        // Read data from the stream
        let bytes_read = match recv_stream.read(&mut temp_read_buf).await {
            Ok(Some(n)) => n,
            Ok(None) => {
                // Stream was gracefully closed by the peer
                debug!("Connection closed by peer while reading from stream.");
                break; // Exit the main while loop
            }
            Err(e) => {
                error!("Error reading from QUIC stream: {}", e);
                return Err(Box::new(e)); // Propagate the QUIC read error
            }
        };

        // If Ok(Some(n)) is returned by quinn::RecvStream::read with a non-empty
        // temp_read_buf, n should be greater than 0. If n is 0, it typically
        // implies an issue or an unexpected state not covered by Ok(None) for
        // EOF. For robustness, though Ok(None) should be the primary EOF
        // indicator, we also break if 0 bytes are read.
        if bytes_read == 0 {
            debug!("Read 0 bytes from stream, assuming connection closed or issue.");
            break;
        }

        receive_buffer.extend_from_slice(&temp_read_buf[..bytes_read]);

        // Process all complete messages in the receive_buffer
        loop {
            if receive_buffer.len() < 4 {
                // Not enough data for the 4-byte length prefix
                break;
            }

            let mut length_bytes = [0u8; 4];
            length_bytes.copy_from_slice(&receive_buffer[0..4]);
            let payload_length = u32::from_be_bytes(length_bytes) as usize;

            // Protect against excessively large payloads
            if payload_length > MAX_PAYLOAD_SIZE {
                error!(
                    "Received payload_length {} exceeds maximum allowed {}",
                    payload_length, MAX_PAYLOAD_SIZE
                );
                return Err(Box::from(format!(
                    "Payload too large: {} bytes, max allowed: {}",
                    payload_length, MAX_PAYLOAD_SIZE
                )));
            }

            let total_message_length = 4 + payload_length;

            if receive_buffer.len() < total_message_length {
                // Not enough data for the full message (prefix + payload)
                break;
            }

            // Extract and deserialize the transaction payload
            let message_data = &receive_buffer[4..total_message_length];
            let tx: VersionedTransaction = match bincode::deserialize(message_data) {
                Ok(tx) => tx,
                Err(e) => {
                    error!("Failed to deserialize transaction: {}", e);
                    // Consider if the connection should be dropped or just this message skipped.
                    // For now, propagating error, which closes this handler.
                    return Err(Box::new(e));
                }
            };

            tx.sanitize()?; // SanitizeError will be propagated by ?

            let signature = *tx
                .signatures
                .first()
                .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from("Transaction has no signatures"))?;

            let sanitized_tx = SanitizedTransaction::try_create(
                tx,
                MessageHash::Compute,
                Some(false), // is_simple_vote_tx
                SimpleAddressLoader::Enabled(LoadedAddresses::default()),
                &HashSet::new(), // reserved_account_keys
            )?; // TransactionError will be propagated by ?

            // Send the sanitized transaction for processing
            sender.send((sanitized_tx, 0))?; // 0 for priority, Crossbeam SendError propagated by ?

            // Update metrics
            metrics.record_tx_size_bytes(payload_length as u64);
            metrics.increase_total_tx_received(1);

            // Send the signature back as an acknowledgement
            send_stream.write_all(signature.as_ref()).await?; // Quinn WriteError propagated by ?

            // Remove the processed message from the buffer
            receive_buffer.drain(0..total_message_length);
        }
    }

    Ok(())
}

const UNSPECIFIED_V4: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
const SERVER_ADDR: SocketAddr = SocketAddr::new(UNSPECIFIED_V4, 5004);

impl QuicTxReceiver {
    pub fn new(tx_sender: Sender<(SanitizedTransaction, u64)>, exit: Arc<AtomicBool>) -> Self {
        let exit_clone = exit.clone();
        let metrics = Arc::new(QuicTxReceiverMetrics::default());

        let metrics_clone = metrics.clone();
        let quic_server = std::thread::Builder::new()
            .name("quicTxReceiver".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("TxRxRT")
                    .build()
                    .expect("Failed to create tokio runtime for streamer");

                let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
                let cert_der = CertificateDer::from(cert.cert);
                let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

                let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert_der], key).unwrap();
                let mut transport_config = quinn::TransportConfig::default();

                let congestion_controller_factory = {
                    let mut cubic_config = quinn::congestion::CubicConfig::default();
                    cubic_config.initial_window(u64::MAX);
                    Arc::new(cubic_config)
                };
                let stream_rwnd = {
                    let expected_rtt = 1u32; // ms
                    let max_stream_bandwidth = 125000 * 1000u32; // 125M bytes/s
                    max_stream_bandwidth / 1000 * expected_rtt
                };
                transport_config.stream_receive_window(stream_rwnd.into());
                transport_config.send_window(8 * stream_rwnd as u64);
                transport_config.congestion_controller_factory(congestion_controller_factory);
                transport_config.keep_alive_interval(Some(Duration::from_secs(1)));
                transport_config.max_idle_timeout(Some(VarInt::from_u32(5_000).into()));

                server_config.transport = Arc::new(transport_config);

                runtime.block_on(async move {
                    let endpoint = quinn::Endpoint::server(server_config, SERVER_ADDR).unwrap();
                    let mut exit_check_ticker = tokio::time::interval(Duration::from_secs(1));
                    loop {
                        tokio::select! {
                            // every second, check if the exit flag is set
                            _ = exit_check_ticker.tick() => {
                                if exit.load(Ordering::Relaxed) {
                                    break;
                                }
                            }
                            Some(incoming) = endpoint.accept() => {
                                let connection = match incoming.await {
                                    Ok(connection) => connection,
                                    Err(e) => {
                                        error!("Error accepting connection: {}", e);
                                        continue;
                                    }
                                };

                                debug!("Connection from {}", connection.remote_address());

                                if !connection.remote_address().ip().to_string().starts_with("10.")
                                    && !connection.remote_address().ip().to_string().starts_with("127.")
                                {
                                    connection.close(quinn::VarInt::from_u32(0x1337), b"fuxk off");
                                    continue;
                                }

                                let tx_sender_clone = tx_sender.clone();
                                let exit_clone = exit.clone();
                                let metrics = metrics_clone.clone();
                                tokio::spawn(async move {
                                    metrics.increase_active_connections();
                                    let _ = handle_connection(connection, tx_sender_clone.clone(), exit_clone.clone(), metrics.clone()).await;
                                    metrics.decrease_active_connections();
                                });
                            }
                        }
                    }
                });
            })
            .unwrap();

        let metrics_clone = metrics.clone();
        let reporting_thread = std::thread::Builder::new()
            .name("quicTxReceiverReporting".to_string())
            .spawn(move || loop {
                metrics_clone.report();
                std::thread::sleep(Duration::from_secs(10));
                if exit_clone.load(Ordering::Relaxed) {
                    break;
                }
            })
            .unwrap();

        Self {
            server: quic_server,
            metrics,
            reporting_thread,
        }
    }

    pub fn join(self) {
        self.server.join().unwrap();
        self.reporting_thread.join().unwrap();
    }

    pub fn get_address(&self) -> SocketAddr {
        SERVER_ADDR
    }
}
