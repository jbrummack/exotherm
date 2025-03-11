use thiserror::Error;

pub type SResult<T> = Result<T, ExothermError>;
#[allow(unused)]
#[derive(Debug, Error)]
pub enum ExothermError {
    #[error("{0}")]
    FoundationDB(#[from] foundationdb::FdbError),
    #[error("{0}")]
    FoundationDBBinding(#[from] foundationdb::FdbBindingError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
    #[error("{0}")]
    TomlError(#[from] toml::de::Error),
    #[error("{0}")]
    Rkvy(#[from] rkyv::rancor::Error),
    #[error("{0}")]
    JsonParse(#[from] serde_json::Error),
    //#[error("{0}")]
    //Storage(#[from] libsql::Error),
    //#[error("{0}")]
    //Surreal(#[from] surrealdb::Error),
    #[error("{0}")]
    Uuid(#[from] uuid::Error),
    #[error("{0}")]
    RowDecode(#[from] ConvertError),
    //#[error("{0}")]
    //Lance(#[from] lancedb::Error),
}

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("CantConvert")]
    CantConvert,
}
