use std::i32;

use uuid::Uuid;

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone)]
pub enum DbValue {
    Bool(bool),
    Int32(i32),
    Enum(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Float(f32),
    Double(f64),
    String(String),
    EnumNumber(i32),
    Vector(Vec<f32>),
    Blob(Vec<u8>),
    Uuid(Uuid),
    None,
}
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone)]
pub struct Row(pub Vec<DbValue>);

pub trait IndexExtractable {
    fn index(&self) -> IndexableValue {
        IndexableValue::None
    }
}

pub trait DbValueEncode {
    fn encode_db(&self) -> DbValue {
        DbValue::None
    }
}

#[derive(Debug, Clone)]
pub enum IndexableValue {
    Bool(bool),
    Int32(i32),
    Enum(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    EnumNumber(i32),
    Vector(Vec<f32>),
    Uuid(Uuid),
    //Blob(Vec<u8>),
    None,
}
impl IndexableValue {
    pub fn bounds(&self) -> (Self, Self) {
        match self {
            IndexableValue::Bool(_) => (Self::Bool(false), Self::Bool(true)),
            IndexableValue::Int32(_) => (Self::Int32(i32::MIN), Self::Int32(i32::MAX)),
            IndexableValue::Enum(_) => (Self::None, Self::None),
            IndexableValue::Int64(_) => (Self::Int64(i64::MIN), Self::Int64(i64::MAX)),
            IndexableValue::UInt32(_) => (Self::UInt32(u32::MIN), Self::UInt32(u32::MAX)),
            IndexableValue::UInt64(_) => (Self::UInt64(u64::MIN), Self::UInt64(u64::MAX)),
            IndexableValue::Float(_) => (Self::Float(f32::MIN), Self::Float(f32::MAX)),
            IndexableValue::Double(_) => (Self::Double(f64::MIN), Self::Double(f64::MAX)),
            IndexableValue::String(_) => (Self::UInt32(u32::MIN), Self::UInt32(u32::MAX)),
            IndexableValue::Uuid(_) => (Self::Uuid(Uuid::nil()), Self::Uuid(Uuid::max())),
            //IndexableValue::EnumNumber(_) => (Self::None, Self::None),
            //IndexableValue::Vector(_) => (Self::None, Self::None),
            IndexableValue::None => (Self::None, Self::None),
            //IndexableValue::Blob(items) => (Self::None, Self::None),
            _ => (Self::None, Self::None),
        }
    }
    pub fn append_to_key(&self, key: &mut Vec<u8>) {
        match self {
            IndexableValue::Bool(bool) => {
                if *bool {
                    key.push(1)
                } else {
                    key.push(0)
                }
            }
            IndexableValue::Int32(int32) => {
                for b in int32.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::Enum(_) => todo!(),
            IndexableValue::Int64(int64) => {
                for b in int64.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::UInt32(uint32) => {
                for b in uint32.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::UInt64(uint64) => {
                for b in uint64.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::Float(float) => {
                let mut bits = float.to_bits();
                if bits & 0x8000_0000 != 0 {
                    bits ^= 0xFFFF_FFFF; // Flip all bits for negative numbers
                } else {
                    bits ^= 0x8000_0000; // Flip only the sign bit for positive numbers
                }
                for b in bits.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::Uuid(uuid) => {
                for b in uuid.as_bytes() {
                    key.push(*b);
                }
            }
            IndexableValue::Double(double) => {
                let mut bits = double.to_bits();
                if bits & 0x8000_0000_0000_0000 != 0 {
                    bits ^= 0xFFFF_FFFF_FFFF_FFFF; // Flip all bits for negative numbers
                } else {
                    bits ^= 0x8000_0000_0000_0000; // Flip only the sign bit for positive numbers
                }
                for b in bits.to_be_bytes() {
                    key.push(b);
                }
            }
            IndexableValue::String(string) => {
                for b in string.as_bytes() {
                    key.push(*b);
                }
            }
            IndexableValue::EnumNumber(_) => (),
            IndexableValue::Vector(_) => (),
            IndexableValue::None => (),
            //IndexableValue::Blob(_items) => (),
        }
    }
}

impl<T> DbValueEncode for Option<T>
where
    T: DbValueEncode,
{
    fn encode_db(&self) -> DbValue {
        if let Some(some) = self {
            some.encode_db()
        } else {
            DbValue::None
        }
    }
}

macro_rules! impl_db_value_encode {
    ($type:ty, $variant:ident) => {
        impl DbValueEncode for $type {
            fn encode_db(&self) -> DbValue {
                DbValue::$variant(self.clone())
            }
        }
    };
}
impl_db_value_encode!(String, String);
impl_db_value_encode!(u32, Uint32);
impl_db_value_encode!(u64, Uint64);
impl_db_value_encode!(i32, Int32);
impl_db_value_encode!(i64, Int64);
impl_db_value_encode!(f32, Float);
impl_db_value_encode!(f64, Double);
impl_db_value_encode!(bool, Bool);
impl_db_value_encode!(Vec<u8>, Blob);
impl_db_value_encode!(Uuid, Uuid);

macro_rules! impl_index_extractable {
    ($type:ty, $variant:ident) => {
        impl IndexExtractable for $type {
            fn index(&self) -> IndexableValue {
                IndexableValue::$variant(self.clone())
            }
        }
    };
}

impl<T> IndexExtractable for Option<T>
where
    T: IndexExtractable,
{
    fn index(&self) -> IndexableValue {
        if let Some(some) = self {
            some.index()
        } else {
            IndexableValue::None
        }
    }
}

impl_index_extractable!(String, String);
impl_index_extractable!(u32, UInt32);
impl_index_extractable!(u64, UInt64);
impl_index_extractable!(i32, Int32);
impl_index_extractable!(i64, Int64);
impl_index_extractable!(f32, Float);
impl_index_extractable!(f64, Double);
impl_index_extractable!(Uuid, Uuid);

/*impl IndexExtractable for VectorI8 {}
impl IndexExtractable for VectorF32 {}
impl IndexExtractable for FtsIndexed {}
impl<T> IndexExtractable for BtreeIndexed<T> where T: IndexRepr {}
*/
