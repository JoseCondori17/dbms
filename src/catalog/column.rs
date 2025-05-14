use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    att_name: String,
    att_type_id: u8,
    att_len: u32,
    att_not_null: bool,
    att_has_def: bool,
}