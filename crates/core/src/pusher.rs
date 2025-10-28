use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, LazyLock, RwLock,
    },
    thread::JoinHandle,
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use infinisvm_logger::{debug, error};
use quinn::Connection;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::metrics::PusherMetrics;

pub static METRICS: LazyLock<PusherMetrics> = LazyLock::new(PusherMetrics::default);

pub struct Pusher {
    _quic_handler: JoinHandle<()>,
    result_receiver: Receiver<(Vec<u8>, Vec<u8>)>,

    connections: Arc<RwLock<Vec<(Connection, Sender<Bytes>)>>>,
}

const UNSPECIFIED_V4: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
const SERVER_ADDR: SocketAddr = SocketAddr::new(UNSPECIFIED_V4, 5003);

async fn handle_connection(
    connection: Connection,
    receiver: Receiver<Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!("Handling connection from {}", connection.remote_address());
    let (mut stream, _) = connection.open_bi().await?;
    debug!("Opened stream");
    loop {
        let data = match receiver.recv() {
            Ok(data) => data,
            Err(e) => {
                error!("Error receiving data: {:#}", e);
                return Err(e.into());
            }
        };

        debug!("Received data");

        let length = data.len() as u32;
        let mut final_data = Vec::with_capacity(4 + data.len());
        final_data.extend_from_slice(&length.to_be_bytes());
        final_data.extend_from_slice(&data);
        stream.write_all(&final_data).await?;
        METRICS.increase_bytes_sent(data.len() as u64);
    }
}

impl Pusher {
    pub fn new(result_receiver: Receiver<(Vec<u8>, Vec<u8>)>) -> Self {
        let connections = Arc::new(RwLock::new(Vec::new()));

        let connections_clone = connections.clone();
        let quic_server = std::thread::Builder::new()
            .name("pusherQuicServer".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("pusherRT1")
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
                    let expected_rtt = 25u32; // ms (25ms) make it higher for bigger send window
                    let max_stream_bandwidth = 125000 * 1000u32; // 125M bytes/s
                    max_stream_bandwidth / 1000 * expected_rtt
                };
                transport_config.stream_receive_window(stream_rwnd.into());
                transport_config.send_window(16 * stream_rwnd as u64);
                transport_config.congestion_controller_factory(congestion_controller_factory);

                server_config.transport = Arc::new(transport_config);

                runtime.block_on(async move {
                    let inner_runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .thread_name("pusherRT2")
                        .build()
                        .expect("Failed to create tokio runtime for streamer");

                    let endpoint = quinn::Endpoint::server(server_config, SERVER_ADDR).unwrap();
                    while let Some(connection) = endpoint.accept().await {
                        let connection = match connection.await {
                            Ok(connection) => connection,
                            Err(e) => {
                                error!("Error accepting connection: {}", e);
                                continue;
                            }
                        };

                        debug!("Connection from {}", connection.remote_address());

                        if !connection.remote_address().ip().to_string().starts_with("10.") &&
                            !connection.remote_address().ip().to_string().starts_with("127.")
                        {
                            connection.close(quinn::VarInt::from_u32(0x1337), b"fuxk off");
                            continue;
                        }

                        let (sender, receiver) = crossbeam_channel::bounded(100_000);
                        connections_clone.write().unwrap().push((connection.clone(), sender));
                        METRICS.increase_active_connections();

                        inner_runtime.spawn(async move {
                            match handle_connection(connection, receiver).await {
                                Ok(_) => (),
                                Err(e) => error!("Error handling connection: {}", e),
                            }
                        });
                    }
                });
            })
            .unwrap();

        Self {
            result_receiver,
            connections,
            _quic_handler: quic_server,
        }
    }

    pub fn run_loop(&mut self, exit: Arc<AtomicBool>) {
        // main loop
        while !exit.load(Ordering::Relaxed) {
            if let Ok((data1, data2)) = self.result_receiver.try_recv() {
                let mut removal_indices = Vec::new();
                let connections = self.connections.read().unwrap();
                debug!("main loop recv {}", connections.len());

                let data: Bytes = bincode::serialize(&(data1.clone(), data2.clone()))
                    .expect("Failed to serialize data")
                    .into();

                for (conn, sender) in connections.iter() {
                    if sender.send(data.clone()).is_err() {
                        removal_indices.push(conn.remote_address());
                    } else {
                        METRICS.increase_bytes_queued(data.len() as u64);
                    }
                }
                drop(connections);

                let mut connections = self.connections.write().unwrap();
                for (conn, _) in connections.iter() {
                    if removal_indices.contains(&conn.remote_address()) {
                        conn.close(quinn::VarInt::from_u32(0x1337), b"bye");
                        METRICS.decrease_active_connections();
                    }
                }
                connections.retain(|(connection, _)| !removal_indices.contains(&connection.remote_address()));
                drop(connections);
            }
        }
        println!("pusher exit");
        // remove all connections
        let connections = self.connections.write().unwrap();
        for (conn, _) in connections.iter() {
            conn.close(quinn::VarInt::from_u32(0x1337), b"sequencer shutdown");
            METRICS.decrease_active_connections();
        }
        drop(connections);
    }
}
