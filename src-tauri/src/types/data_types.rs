use std::io::Read;
use datafusion::arrow::datatypes::ToByteSlice;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum DataTypeTag {
    SMALLINT    = 1,
    INT         = 2,
    BIGINT      = 3,
    DOUBLE      = 4,
    CHAR        = 5,
    VARCHAR     = 6,
    BOOLEAN     = 7,
    UUID        = 8,
    DATE        = 9,
    TIME        = 10,
    TIMESTAMP   = 11, // WITH TIME ZONE 
    GEOMETRIC   = 12,
}

#[derive(Debug, Clone)]
pub enum DataValue {
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Double(f64),
    Char(Vec<u8>),
    VarChar(Vec<u8>),
    Bool(bool),
    Uuid([u8; 16]),
    Date(i32),
    Time(i64),
    Timestamp(i64),
}

impl DataValue {
    pub fn to_bytes(&self, att_len: u32) -> Vec<u8> {
        match self {
            // LittleEndian
            DataValue::SmallInt(v) => v.to_le_bytes().to_vec(),
            DataValue::Int(v)      => v.to_le_bytes().to_vec(),
            DataValue::BigInt(v)   => v.to_le_bytes().to_vec(),
            DataValue::Double(v)   => v.to_le_bytes().to_vec(),
            DataValue::Bool(b)     => vec![*b as u8],
            DataValue::Uuid(arr)   => arr.to_vec(),
            DataValue::Date(d)     => d.to_le_bytes().to_vec(),
            DataValue::Time(t)     => t.to_le_bytes().to_vec(),
            DataValue::Timestamp(t)=> t.to_le_bytes().to_vec(),
            DataValue::Char(bytes) | DataValue::VarChar(bytes) => {
                let mut buf = vec![0u8; att_len as usize];
                let n = bytes.len().min(att_len as usize);
                buf[..n].copy_from_slice(&bytes[..n]);
                buf
            }
        }
    }
    pub fn from_bytes(bytes: &[u8], type_id: u8, len: u32) -> Self {
        match type_id {
            1 => {
                // SmallInt (i16)
                let value = i16::from_le_bytes([bytes[0], bytes[1]]);
                DataValue::SmallInt(value)
            },
            2 => {
                // Int (i32)
                let value = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                DataValue::Int(value)
            },
            3 => {
                // BigInt (i64)
                let mut b = [0u8; 8];
                b.copy_from_slice(&bytes[0..8]);
                let value = i64::from_le_bytes(b);
                DataValue::BigInt(value)
            },
            4 => {
                // Double (f64)
                let mut b = [0u8; 8];
                b.copy_from_slice(&bytes[0..8]);
                let value = f64::from_le_bytes(b);
                DataValue::Double(value)
            },
            5 => {
                // Char (fixed length)
                DataValue::Char(bytes.to_vec())
            },
            6 => {
                // VarChar (variable length)
                DataValue::VarChar(bytes.to_vec())
            },
            7 => {
                // Bool
                let value = bytes[0] != 0;
                DataValue::Bool(value)
            },
            8 => {
                // UUID
                let mut b = [0u8; 16];
                b.copy_from_slice(&bytes[0..16]);
                DataValue::Uuid(b)
            },
            9 => {
                // Date
                let mut b = [0u8; 4];
                b.copy_from_slice(&bytes[0..4]);
                let value = i32::from_le_bytes(b);
                DataValue::Date(value)
            },
            10 => {
                // Time
                let mut b = [0u8; 8];
                b.copy_from_slice(&bytes[0..8]);
                let value = i64::from_le_bytes(b);
                DataValue::Time(value)
            },
            11 => {
                // Timestamp
                let mut b = [0u8; 8];
                b.copy_from_slice(&bytes[0..8]);
                let value = i64::from_le_bytes(b);
                DataValue::Timestamp(value)
            },
            _ => panic!("Invalid type id: {}", type_id),
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            DataValue::SmallInt(v) => v.to_string(),
            DataValue::Int(v) => v.to_string(),
            DataValue::BigInt(v) => v.to_string(),
            DataValue::Double(v) => v.to_string(),
            DataValue::Char(v) => String::from_utf8_lossy(v).to_string(),
            DataValue::VarChar(v) => String::from_utf8_lossy(v).to_string(),
            DataValue::Bool(v) => v.to_string(),
            DataValue::Uuid(v) => {
                let uuid = uuid::Uuid::from_bytes(v.clone().try_into().unwrap_or([0; 16]));
                uuid.to_string()
            },
            DataValue::Date(v) => v.to_string(), // Idealmente convertir a formato fecha
            DataValue::Time(v) => v.to_string(), // Idealmente convertir a formato hora
            DataValue::Timestamp(v) => v.to_string(), // Idealmente convertir a formato timestamp
        }
    }
}