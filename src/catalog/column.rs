use datafusion::arrow::datatypes::{Field, DataType, TimeUnit};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    att_name: String,
    att_type_id: u8,
    att_len: u32,
    att_not_null: bool,
    att_has_def: bool,
}

impl Column {
    pub fn new(
        att_name: String,
        att_type_id: u8,
        att_len: u32,
        att_not_null: bool,
        att_has_def: bool,
    ) -> Self {
        Self {
            att_name,
            att_type_id,
            att_len,
            att_not_null,
            att_has_def,
        }
    }
    pub fn get_att_name(&self) -> String {
        self.att_name.clone()
    }
    pub fn get_att_type_id(&self) -> u8 {
        self.att_type_id
    }
    pub fn get_att_len(&self) -> u32 {
        self.att_len
    }
    pub fn get_att_not_null(&self) -> bool {
        self.att_not_null
    }
    pub fn get_att_has_def(&self) -> bool {
        self.att_has_def
    }

    pub fn to_arrow_field(&self) -> Field {
        let data_type = match self.att_type_id {
            1 => DataType::Int16,                        // SMALLINT
            2 => DataType::Int32,                        // INT
            3 => DataType::Int64,                        // BIGINT
            4 => DataType::Float64,                      // DOUBLE
            5 => DataType::Utf8,                         // CHAR
            6 => DataType::Utf8,                         // VARCHAR
            7 => DataType::Boolean,                      // BOOLEAN
            8 => DataType::Utf8,                         // UUID
            9 => DataType::Date32,                       // DATE
            10 => DataType::Time64(TimeUnit::Microsecond), // TIME
            11 => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), // TIMESTAMP WITH TIME ZONE
            12 => DataType::Utf8,                        // GEOMETRIC
            _ => panic!("Unsupported data type"),
        };
        
        Field::new(
            self.att_name.clone(),
            data_type,
            !self.att_not_null
        )
    }
}