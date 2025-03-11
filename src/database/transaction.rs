use foundationdb::{FdbBindingError, RangeOption, options::StreamingMode};
use uuid::Uuid;

use crate::database::record::{IndexAddress, RecordStruct};

#[allow(dead_code)]
pub struct STransaction {
    pub(super) trx: foundationdb::RetryableTransaction,
    pub maybe_commited: bool,
    pub(super) tenant: &'static str,
}
#[allow(dead_code)]
pub enum Query {
    Equal(IndexAddress),
    Between(IndexAddress, IndexAddress),
    Gt(IndexAddress),
    Lt(IndexAddress),
    WantAll(IndexAddress),
}
impl Query {
    fn into_range(&self) -> Range {
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
    }
}

pub struct Range(IndexAddress, IndexAddress);

#[allow(unused)]
#[derive(Debug)]
pub struct PageResult<'a> {
    pub ids: Vec<Uuid>,
    pub used_bandwidth: usize,
    next: Option<RangeOption<'a>>,
}

impl STransaction {
    fn tenant(&self) -> Vec<u8> {
        self.tenant.as_bytes().to_vec()
    }
    pub async fn clear_value<T: RecordStruct<Decoded = T>>(
        &self,
        pk: Uuid,
    ) -> Result<bool, FdbBindingError> {
        let mut key = self.tenant();
        T::get_corpus_key(&mut key, pk);
        //println!("GET: {:?}", key);
        if let Some(value) = &self.trx.get(&key, false).await? {
            //println!("GET VALUE {:?}", value.to_vec());
            let d =
                T::decode(&value).map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
            let indices = d.indices(pk);
            for index in indices {
                self.clear_index(index);
            }
            self.clear_corpus(pk, &d);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn get_value<T: RecordStruct<Decoded = T>>(
        &self,
        pk: Uuid,
    ) -> Result<Option<T>, FdbBindingError> {
        let mut key = self.tenant();
        T::get_corpus_key(&mut key, pk);
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
        //trx: &foundationdb::Transaction,
        record: &impl RecordStruct,
        pk: Uuid,
    ) -> Result<(), FdbBindingError> {
        let new_indices = record.indices(pk);
        for index in new_indices {
            self.set_index(index, pk);
        }
        self.set_corpus(pk, record)?;
        Ok(())
    }
    fn calculate_index_key(&self, index: IndexAddress) -> Vec<u8> {
        let mut key = Vec::<u8>::new();
        key.push(84); //Tenant prefix
        for b in self.tenant.as_bytes() {
            key.push(*b);
        }
        for b in index.into_key() {
            key.push(b);
        }
        key
    }
    fn set_index(&self, index: IndexAddress, pk: Uuid) {
        let value = pk.as_bytes();
        let key = self.calculate_index_key(index);
        //println!("{}{:?}", self.tenant, index.into_key());
        println!("Index {}->{value:?}", String::from_utf8_lossy(&key));
        self.trx.set(&key, value);
    }
    fn clear_index(&self, index: IndexAddress) {
        let key = self.calculate_index_key(index);
        //println!("{}{:?}", self.tenant, index.into_key());
        //println!("Index {}->{value:?}", String::from_utf8_lossy(&key));
        self.trx.clear(&key);
    }
    pub async fn query_index(
        &self,
        query: Query,
        reverse: bool,
    ) -> Result<PageResult, FdbBindingError> {
        let Range(from, to) = query.into_range();
        let from = self.calculate_index_key(from);
        let to = self.calculate_index_key(to);
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
            //value.value()
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
    fn corpus_key(&self, pk: Uuid, record: &impl RecordStruct) -> Vec<u8> {
        let mut crp_key = self.tenant();
        record.append_corpus_key(&mut crp_key, pk);
        crp_key
    }
    fn clear_corpus(&self, pk: Uuid, record: &impl RecordStruct) {
        let crp_key = self.corpus_key(pk, record);
        self.trx.clear(&crp_key);
    }
    fn set_corpus(&self, pk: Uuid, record: &impl RecordStruct) -> Result<(), FdbBindingError> {
        let crp_value: rkyv::util::AlignedVec<16> = record
            .serialize()
            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
        let crp_key = self.corpus_key(pk, record);
        self.trx.set(&crp_key, &crp_value);
        Ok(())
    }
}
