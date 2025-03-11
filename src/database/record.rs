use uuid::Uuid;

use crate::{
    database::values_indices::{ArchivedRow, DbValue, IndexableValue, Row},
    error::{ConvertError, SResult},
};

pub trait RecordStruct {
    type Decoded;
    fn append_corpus_key(&self, key: &mut Vec<u8>, pk: Uuid) {
        Self::get_corpus_key(key, pk);
    }
    fn get_corpus_key(key: &mut Vec<u8>, pk: Uuid) {
        key.push(67); //Corpus magic number
        for b in Self::name().as_bytes() {
            key.push(*b);
        }
        key.push(29);
        for b in pk.as_bytes() {
            key.push(*b);
        }
    }
    fn serialize(&self) -> Result<rkyv::util::AlignedVec, rkyv::rancor::Error> {
        let row = Row(self.corpus());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)?;
        Ok(bytes)
    }
    fn name() -> &'static str;
    fn corpus(&self) -> Vec<DbValue>; //Result<rkyv::util::AlignedVec, rkyv::rancor::Error>;
    fn indices(&self, uuid: uuid::Uuid) -> Vec<IndexAddress>; //Vec<(usize, crate::values_indices::IndexableValue)>;
    fn tname(&self) -> &'static str;
    fn deserialize(from: Vec<DbValue>) -> Result<Self::Decoded, ConvertError>;
    fn decode(from: &[u8]) -> SResult<Self::Decoded> {
        let mut aligned: rkyv::util::AlignedVec<16> =
            rkyv::util::AlignedVec::with_capacity(from.len());
        aligned.extend_from_slice(from);
        let row = rkyv::access::<ArchivedRow, rkyv::rancor::Error>(&aligned)?;

        let Row(row) = rkyv::deserialize::<Row, rkyv::rancor::Error>(row)?;
        let deserialize = Self::deserialize(row)?;
        Ok(deserialize)
    }
}
pub fn pad_indices(input: Vec<(usize, DbValue)>) -> Vec<DbValue> {
    let mut max = 0;
    for (idx, _) in &input {
        if *idx > max {
            max = *idx;
        }
    }
    let mut padded = vec![DbValue::None; max + 1];
    for (idx, iv) in input {
        padded[idx] = iv;
    }
    padded
}

#[macro_export]
macro_rules! index_value_optional {
    ($field:expr, Btree) => {
        $field.btree_index(),
    };
    // Add other index types as needed
    ($field:expr, ) => {}; // Default case with no index type
}
#[allow(dead_code)]
pub struct CompactRow {
    fields: Vec<u32>,
    values: Vec<DbValue>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct QueryRange {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct IndexAddress {
    pub table: &'static str,
    pub row: Uuid,
    pub idx: usize,
    pub value: IndexableValue,
}
#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct BlobstoreAddress {
    bucket: String,
    id: Uuid,
}

impl IndexAddress {
    pub fn into_key(&self) -> Vec<u8> {
        let len = self.table.len() + 32;
        let mut key = Vec::<u8>::with_capacity(len);
        key.push(29); //Index magic number
        for b in self.table.as_bytes() {
            key.push(*b);
        }
        key.push(29);
        for b in self.idx.to_be_bytes() {
            key.push(b);
        }
        key.push(29);
        self.value.append_to_key(&mut key);
        key.push(29);
        for b in self.row.as_bytes() {
            key.push(*b);
        }
        key
    }
}

//($name:ident { $($field_num:literal -> $field:ident :  [$($index_name:ident : $index_type:ty)?]  $ty:ty ),* $(,)? })
#[macro_export]
macro_rules! record {
    ($name:ident { $($field_num:literal -> $field:ident :  [$($index_name:ident)?]  $ty:ty ),* $(,)? }) => {
        #[derive(Debug)]
        pub struct $name {
            $(pub $field: $ty),*
        }
        impl $name {

            $(
                $(
                    fn $index_name(row: uuid::Uuid, value: &$ty) -> $crate::database::record::IndexAddress{
                        use $crate::database::record::RecordStruct;
                        use $crate::database::values_indices::*;
                        $crate::database::record::IndexAddress {
                            table: Self::name(),
                            row,
                            idx: $field_num,
                            value: value.index()
                        }
                    }
                )?
            )*
        }
        impl $crate::database::record::RecordStruct for $name {
            type Decoded = $name;
            fn name() -> &'static str {
                stringify!($name)
            }
            fn corpus(&self) -> Vec<$crate::database::values_indices::DbValue>
            {
                use $crate::database::values_indices::*;
                let unpadded = vec![
                    $(($field_num,self.$field.encode_db())),*
                ];
                let padded = $crate::database::record::pad_indices(unpadded);
                padded
            }
            fn indices(&self, row: uuid::Uuid) ->  Vec<$crate::database::record::IndexAddress>//Vec<(usize, IndexableValue)>
            {
                vec![$(
                    $(
                        //Self::$index_name($crate::record::Query::GetIndex(self.$field.clone(), row)),
                        Self::$index_name(row, &self.$field),
                    )?
                )*]
            }
            fn tname(&self) -> &'static str {
                stringify!($name)
            }
            fn deserialize(from: Vec<$crate::database::values_indices::DbValue>) -> Result<Self, $crate::error::ConvertError> {
                let res = $name {
                    $($field: from[$field_num].clone().try_into()?),*
                };
                Ok(res)
            }
        }
    };
}
