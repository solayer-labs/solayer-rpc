use std::path::PathBuf;

#[derive(clap::Args)]
pub struct DbArgs {
    #[clap(long, default_value = "10.20.30.215")]
    pub clickhouse_host: String,
    #[clap(long, default_value = "9000")]
    pub clickhouse_port: u16,
    #[clap(long, default_value = "10")]
    pub clickhouse_connections_per_instance: usize,
    #[clap(long, default_value = "10")]
    pub clickhouse_instances: usize,

    #[clap(long, default_value = "10.20.30.214")]
    pub cassandra_host: String,
    #[clap(long, default_value = "9042")]
    pub cassandra_port: u16,

    #[clap(long, default_value = "/mnt/data/slots")]
    pub s3_path: PathBuf,

    #[clap(long)]
    pub s3_access_key_id: Option<String>,

    #[clap(long)]
    pub s3_secret_key: Option<String>,
}
