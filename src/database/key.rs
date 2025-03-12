use uuid::Uuid;

use crate::error::SResult;

use super::values_indices::IndexableValue;

static MAGIC_NUMBER: u8 = 99;

pub struct Key {
    tenant: Tenant,
    table: &'static str,
    purpose: Purpose,
    row: Uuid,
}

pub enum Tenant {
    Named(&'static str),
    Id(Uuid),
    Unset,
}

impl Tenant {
    fn append(&self, key: &mut Vec<u8>) -> SResult<()> {
        match self {
            Tenant::Named(name) => {
                for b in name.as_bytes() {
                    key.push(*b);
                }
                Ok(())
            }
            Tenant::Id(uuid) => {
                for b in uuid.as_bytes() {
                    key.push(*b);
                }
                Ok(())
            }
            Tenant::Unset => Err(crate::error::ExothermError::TenantError),
        }
    }
}

impl Key {
    pub fn new_row(tenant: Tenant, table: &'static str, row: Uuid) -> Self {
        Key {
            tenant,
            table,
            purpose: Purpose::Row,
            row,
        }
    }
    pub fn new_index(
        tenant: Tenant,
        table: &'static str,
        id: u16,
        value: IndexableValue,
        row: Uuid,
    ) -> Self {
        Key {
            tenant,
            table,
            purpose: Purpose::Index(id, value),
            row,
        }
    }
    pub fn generate(&self) -> SResult<Vec<u8>> {
        //assert_ne!(self.tenant, "invalid");
        let mut key = Vec::<u8>::with_capacity(128);
        key.push(MAGIC_NUMBER);
        self.tenant.append(&mut key)?;
        /*for b in self.tenant.as_bytes() {
            key.push(*b);
        }*/
        key.push(0);
        for b in self.table.as_bytes() {
            key.push(*b);
        }
        key.push(0);
        self.purpose.append(&mut key);
        key.push(0);
        for b in self.row.as_bytes() {
            key.push(*b);
        }

        Ok(key)
    }
}

pub enum Purpose {
    Row,                        //Stores the row corpus
    Index(u16, IndexableValue), //Stores the index,
    Blob(&'static str, u16),    //Stores the blob bucket
}

impl Purpose {
    fn append(&self, key: &mut Vec<u8>) {
        match self {
            Purpose::Row => key.push(1),
            Purpose::Index(_, _indexable_value) => key.push(2),
            Purpose::Blob(_, _) => key.push(3),
        }
        match self {
            Purpose::Row => (),
            Purpose::Index(index_col, indexable_value) => {
                let [b1, b2] = index_col.to_be_bytes();
                key.push(b1);
                key.push(b2);
                indexable_value.append_to_key(key);
            }
            Purpose::Blob(bucket, shard) => {
                for b in bucket.as_bytes() {
                    key.push(*b);
                }
                let [b1, b2] = shard.to_be_bytes();
                key.push(b1);
                key.push(b2);
            }
        }
    }
}
