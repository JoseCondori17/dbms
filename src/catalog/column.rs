use sqlparser::ast::{DataType, Value};

pub struct Column {
    name: String,
    data_type: DataType,
    nullable: bool,
    default_value: Option<Value>
}