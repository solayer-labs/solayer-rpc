use std::path::PathBuf;

#[derive(clap::Args)]
pub struct DbArgs {
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

    #[clap(long, default_value = "us-east-1")]
    pub s3_region: String,
}
