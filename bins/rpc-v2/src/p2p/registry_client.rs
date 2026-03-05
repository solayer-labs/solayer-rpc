use eyre::Result;
use infinisvm_registry::RpcPeerInfo;
use infinisvm_sync::http_client::HttpClient;

pub struct RegistryClient {
    base_url: String,
    http: HttpClient,
}

impl RegistryClient {
    pub fn new(base_url: String) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        Self {
            http: HttpClient::new(base_url.clone()),
            base_url,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn list(&self) -> Result<Vec<RpcPeerInfo>> {
        self.http.get_rpc_set().await
    }
}
