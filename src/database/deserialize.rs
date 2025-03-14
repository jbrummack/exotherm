use crate::{database::values_indices::DbValue, error::ConvertError};

impl TryFrom<DbValue> for String {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::String(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Vec<u8> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Blob(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Vec<f32> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Vector(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}
impl TryFrom<DbValue> for bool {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Bool(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<String> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::String(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for u32 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Uint32(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}
impl TryFrom<DbValue> for Option<u32> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Uint32(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for u64 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Uint64(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<u64> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Uint64(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for i64 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Int64(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<i64> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Int64(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for i32 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Int32(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<i32> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Int32(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for f64 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Double(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<f64> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Double(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}

impl TryFrom<DbValue> for f32 {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        if let DbValue::Float(value) = value {
            Ok(value)
        } else {
            Err(ConvertError::CantConvert { from: value })
        }
    }
}

impl TryFrom<DbValue> for Option<f32> {
    type Error = ConvertError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        match value {
            DbValue::Float(value) => Ok(Some(value)),
            DbValue::None => Ok(None),
            _ => Err(ConvertError::CantConvert { from: value }),
        }
    }
}
