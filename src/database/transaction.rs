use foundationdb::{FdbBindingError, RangeOption, options::StreamingMode};
use uuid::Uuid;

use crate::{
    database::{key::Purpose, record::RecordStruct},
    error::{ExothermError, SResult},
};

use super::key::{Key, Tenant};

#[allow(dead_code)]
pub struct STransaction {
    pub(super) trx: foundationdb::RetryableTransaction,
    pub maybe_commited: bool,
    pub(super) tenant: Tenant,
}
#[allow(dead_code)]
pub enum Query {
    Equal(Key),
    Between(Key, Key),
    Gt(Key),
    Lt(Key),
    WantAll(Key),
}
impl Query {
    fn into_range(self, tenant: Tenant) -> SResult<Range> {
        match self {
            Query::Equal(Key {
                tenant: _,
                table,
                purpose,
                row: _,
            }) => {
                if let Purpose::Index(id, value) = purpose {
                    let from = Key::new_index(tenant, table, id, value.clone(), Uuid::nil());
                    let to = Key::new_index(tenant, table, id, value, Uuid::max());
                    Ok(Range(from, to))
                } else {
                    Err(ExothermError::IndexKeyError)
                }
            }
            Query::Between(key, key1) => {
                if let (
                    Key {
                        purpose: Purpose::Index(id, value1),
                        tenant: _,
                        table,
                        row: _,
                    },
                    Key {
                        purpose: Purpose::Index(id1, value2),
                        tenant: _,
                        table: _,
                        row: _,
                    },
                ) = (key, key1)
                {
                    if id != id1 {
                        return Err(ExothermError::UnequalColumns);
                    }
                    let from = Key::new_index(tenant, table, id, value1, Uuid::nil());
                    let to = Key::new_index(tenant, table, id, value2, Uuid::max());
                    Ok(Range(from, to))
                } else {
                    Err(ExothermError::IndexKeyError)
                }
            }
            Query::Gt(Key {
                tenant: _,
                table,
                purpose,
                row: _,
            }) => {
                if let Purpose::Index(id, value) = purpose {
                    let (_, max) = value.bounds();
                    let from = Key::new_index(tenant, table, id, value, Uuid::nil());
                    let to = Key::new_index(tenant, table, id, max, Uuid::max());
                    Ok(Range(from, to))
                } else {
                    Err(ExothermError::IndexKeyError)
                }
            }
            Query::Lt(Key {
                tenant: _,
                table,
                purpose,
                row: _,
            }) => {
                if let Purpose::Index(id, value) = purpose {
                    let (min, _) = value.bounds();
                    let from = Key::new_index(tenant, table, id, min, Uuid::nil());
                    let to = Key::new_index(tenant, table, id, value, Uuid::max());
                    Ok(Range(from, to))
                } else {
                    Err(ExothermError::IndexKeyError)
                }
            }
            Query::WantAll(Key {
                tenant: _,
                table,
                purpose,
                row: _,
            }) => {
                if let Purpose::Index(id, value) = purpose {
                    let (min, max) = value.bounds();
                    let from = Key::new_index(tenant, table, id, min, Uuid::nil());
                    let to = Key::new_index(tenant, table, id, max, Uuid::max());
                    Ok(Range(from, to))
                } else {
                    Err(ExothermError::IndexKeyError)
                }
            }
        }
        //todo!()
    }
    /*fn into_range(&self, tenant: Tenant) -> Range {
        match self {
            Query::Equal(index_address) => {
                let IndexAddress {
                    table,
                    row: _,
                    idx,
                    value,
                } = index_address;
                Range(
                    IndexAddress {
                        table,
                        row: Uuid::nil(),
                        idx: *idx,
                        value: value.clone(),
                    },
                    IndexAddress {
                        table,
                        row: Uuid::max(),
                        idx: *idx,
                        value: value.clone(),
                    },
                )
            }
            Query::Between(index_address, index_address1) => {
                let IndexAddress {
                    table,
                    row: _,
                    idx,
                    value,
                } = index_address;
                let from = IndexAddress {
                    table,
                    row: Uuid::nil(),
                    idx: *idx,
                    value: value.clone(),
                };
                let IndexAddress {
                    table,
                    row: _,
                    idx,
                    value,
                } = index_address1;
                let to = IndexAddress {
                    table,
                    row: Uuid::max(),
                    idx: *idx,
                    value: value.clone(),
                };
                Range(from, to)
            }
            Query::Gt(index_address) => {
                let (_min, max) = index_address.value.bounds();
                let from = index_address.clone();
                let to = IndexAddress {
                    table: index_address.table,
                    row: Uuid::max(),
                    idx: index_address.idx,
                    value: max,
                };

                Range(from, to)
            }
            Query::Lt(index_address) => {
                let (min, _max) = index_address.value.bounds();
                let from = IndexAddress {
                    table: index_address.table,
                    row: Uuid::nil(),
                    idx: index_address.idx,
                    value: min,
                };
                let to = index_address.clone();

                Range(from, to)
            }
            Query::WantAll(index_address) => {
                let (min, max) = index_address.value.bounds();
                let IndexAddress {
                    table,
                    row: _,
                    idx,
                    value: _,
                } = index_address;
                let from = IndexAddress {
                    table,
                    row: Uuid::nil(),
                    idx: *idx,
                    value: min,
                };
                let to = IndexAddress {
                    table,
                    row: Uuid::max(),
                    idx: *idx,
                    value: max,
                };
                Range(from, to)
            }
        }
    }*/
}

pub struct Range(Key, Key);

#[allow(unused)]
#[derive(Debug)]
pub struct PageResult<'a> {
    pub ids: Vec<Uuid>,
    pub used_bandwidth: usize,
    next: Option<RangeOption<'a>>,
}

impl STransaction {
    pub async fn clear_value<T: RecordStruct<Decoded = T>>(
        &self,
        pk: Uuid,
    ) -> Result<bool, FdbBindingError> {
        let key = T::corpus_key(self.tenant, pk)
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        //println!("GET: {:?}", key);
        if let Some(value) = &self.trx.get(&key, false).await? {
            //println!("GET VALUE {:?}", value.to_vec());
            let d =
                T::decode(&value).map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
            let indices = d.indices(pk);
            for index in indices {
                self.clear_index(index)?;
            }
            self.clear_corpus(pk, &d)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn get_value<T: RecordStruct<Decoded = T>>(
        &self,
        pk: Uuid,
    ) -> Result<Option<T>, FdbBindingError> {
        let key = T::corpus_key(self.tenant, pk)
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        //println!("GET: {:?}", key);
        if let Some(value) = &self.trx.get(&key, false).await? {
            //println!("GET VALUE {:?}", value.to_vec());
            let d =
                T::decode(&value).map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
            Ok(Some(d))
        } else {
            Ok(None)
        }
    }
    pub async fn put_value(
        &self,
        record: &impl RecordStruct,
        pk: Uuid,
    ) -> Result<(), FdbBindingError> {
        let new_indices = record.indices(pk);
        for index in new_indices {
            self.set_index(index, pk)?
        }
        self.set_corpus(pk, record)?;
        Ok(())
    }
    fn generate_index_key(&self, index: Key) -> Result<Vec<u8>, FdbBindingError> {
        let mut key = index;
        key.tenant = self.tenant;
        let key = key
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        Ok(key)
    }
    fn set_index(&self, index: Key, pk: Uuid) -> Result<(), FdbBindingError> {
        let value = pk.as_bytes();
        let key = self.generate_index_key(index)?;
        //println!("{}{:?}", self.tenant, index.into_key());
        println!("Index {}->{value:?}", String::from_utf8_lossy(&key));
        self.trx.set(&key, value);
        Ok(())
    }
    fn clear_index(&self, index: Key) -> Result<(), FdbBindingError> {
        let key = self.generate_index_key(index)?;
        //println!("{}{:?}", self.tenant, index.into_key());
        //println!("Index {}->{value:?}", String::from_utf8_lossy(&key));
        self.trx.clear(&key);
        Ok(())
    }
    pub async fn query_index(
        &self,
        query: Query,
        reverse: bool,
    ) -> Result<PageResult, FdbBindingError> {
        let Range(from, to) = query
            .into_range(self.tenant)
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        let from = from
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        let to = to
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        let mut opt = RangeOption::from((from, to));
        opt.mode = StreamingMode::Iterator;
        opt.reverse = reverse;
        let range = self.trx.get_range(&opt, 5000, true).await?;
        let mut used_bandwidth: usize = 0;
        let mut ids = Vec::<Uuid>::new();
        for kv in &range {
            used_bandwidth += kv.key().len();
            used_bandwidth += kv.value().len();
            let record_id = Uuid::from_slice(kv.value())
                .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
            ids.push(record_id);
        }
        let next = opt.next_range(&range);
        let page = PageResult {
            ids,
            used_bandwidth,
            next,
        };
        Ok(page)
    }
    fn clear_corpus(&self, pk: Uuid, record: &impl RecordStruct) -> Result<(), FdbBindingError> {
        let crp_key = record
            .get_corpus_key(self.tenant, pk)
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        //let crp_key = self.corpus_key(pk, record);
        self.trx.clear(&crp_key);
        Ok(())
    }
    fn set_corpus(&self, pk: Uuid, record: &impl RecordStruct) -> Result<(), FdbBindingError> {
        let crp_value: rkyv::util::AlignedVec<16> = record
            .serialize()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        let crp_key = record
            .get_corpus_key(self.tenant, pk)
            .generate()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        self.trx.set(&crp_key, &crp_value);
        Ok(())
    }
}
