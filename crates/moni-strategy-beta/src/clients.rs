use anyhow::{Context, Result};
use moni_proto::monitor as monitor_pb;
use moni_proto::monitor::monitor_client::MonitorClient;
use moni_proto::store::v1 as store_pb;
use moni_proto::store::v1::store_client::StoreClient;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

pub struct Monitor {
    client: MonitorClient<Channel>,
    authorization: MetadataValue<tonic::metadata::Ascii>,
    tenant_id: String,
}

impl Monitor {
    pub async fn connect(endpoint: String, api_key: &str, tenant_id: String) -> Result<Self> {
        let client = MonitorClient::connect(endpoint)
            .await
            .context("connecting Monitor")?;
        let authorization = format!("Bearer {api_key}")
            .parse()
            .context("Monitor key contains invalid metadata characters")?;
        Ok(Self {
            client,
            authorization,
            tenant_id,
        })
    }

    pub async fn executions(
        &mut self,
        strategy_id: &str,
        limit: u32,
    ) -> Result<Vec<monitor_pb::CompleteSetExecution>> {
        let mut request = Request::new(monitor_pb::ListCompleteSetExecutionsRequest {
            tenant: Some(monitor_pb::TenantRef {
                tenant_id: self.tenant_id.clone(),
            }),
            strategy_id: strategy_id.to_owned(),
            signal_id: String::new(),
            terminal_only: false,
            limit,
        });
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        Ok(self
            .client
            .list_complete_set_executions(request)
            .await
            .context("listing complete-set executions")?
            .into_inner()
            .executions)
    }
}

pub struct Store {
    client: StoreClient<Channel>,
}

impl Store {
    pub async fn connect(endpoint: String) -> Result<Self> {
        Ok(Self {
            client: StoreClient::connect(endpoint)
                .await
                .context("connecting moni-store")?,
        })
    }

    pub async fn snapshots(
        &mut self,
        token_ids: Vec<String>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<store_pb::BookSnapshot>> {
        let mut stream = self
            .client
            .stream_book_snapshots_range(store_pb::StreamBookSnapshotsRangeRequest {
                token_ids,
                from_ms,
                to_ms,
            })
            .await
            .context("querying moni-store book snapshots")?
            .into_inner();
        let mut snapshots = Vec::new();
        while let Some(snapshot) = stream
            .message()
            .await
            .context("reading moni-store book snapshots")?
        {
            snapshots.push(snapshot);
        }
        Ok(snapshots)
    }
}
