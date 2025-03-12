use foundationdb::{FdbBindingError, api::NetworkAutoStop};
//use uuid::Uuid;

use crate::{database::transaction::STransaction, error::SResult};

pub struct Database {
    _autodrop: NetworkAutoStop,
    tenant: Tenant,
    fdb: foundationdb::Database,
}

/*pub struct Page {
    tname: &'static str,
    pks: Vec<Uuid>,
    next: Option<IndexAddress>,
}*/

use serde::{Deserialize, Serialize};

use super::key::Tenant;

//use super::error::DbError;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct FdbStatus {
    #[serde(rename = "ClusterID")]
    cluster_id: String,
    commit_proxies: Vec<Option<serde_json::Value>>,
    connections: Vec<Connection>,
    coordinators: Vec<String>,
    current_coordinator: String,
    grv_proxies: Vec<Option<serde_json::Value>>,
    healthy: bool,
    num_connections_failed: i64,
    storage_servers: Vec<Option<serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Connection {
    address: String,
    bytes_received: i64,
    bytes_sample_time: f64,
    bytes_sent: i64,
    compatible: bool,
    connect_failed_count: i64,
    last_connect_time: f64,
    ping_count: i64,
    ping_timeout_count: i64,
    status: String,
}

#[allow(dead_code)]
impl Database {
    ///Get information about the Database
    pub async fn get_status(&self) -> SResult<FdbStatus> {
        let status = self.fdb.get_client_status().await?;
        let value: FdbStatus = serde_json::from_slice(&status)?;
        Ok(value)
    }
    ///Boot foundationdb and create a database
    pub async fn new(tenant: Tenant) -> SResult<Self> {
        let db = Database {
            _autodrop: unsafe { foundationdb::boot() },
            tenant,
            fdb: foundationdb::Database::default()?,
        };
        Ok(db)
    }
    ///Start a transaction
    ///
    /// ```ignore
    ///     let db = Database::new(database::key::Tenant::Named("testing")).await.unwrap();
    ///     let id = Uuid::new_v4();
    ///     let person = Person {
    ///         name: String::from("NameNameNameNameNamevName"),
    ///         password: String::from("TestTestTestTestTest"),
    ///     };
    ///
    ///     db.transact(|transaction| {
    ///         let person = &person;
    ///         async move {
    ///             transaction.put_value(person, id).await?;
    ///             Ok(())
    ///         }
    ///     })
    ///     .await.unwrap();
    ///
    /// ```
    pub async fn transact<F, Fut, T>(&self, closure: F) -> Result<T, FdbBindingError>
    where
        F: Fn(STransaction) -> Fut,
        Fut: Future<Output = Result<T, FdbBindingError>>,
    {
        self.fdb
            .run(|trx, maybe_committed| async {
                let st = STransaction {
                    trx,
                    maybe_commited: maybe_committed.into(),
                    tenant: self.tenant,
                };
                closure(st).await
            })
            .await
    }
}
