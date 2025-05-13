use sqlparser::ast::{DataType, Value};

pub struct Column {
    name: String,
    data_type: u8,
    nullable: bool,
    default_value: Option<Value>
}