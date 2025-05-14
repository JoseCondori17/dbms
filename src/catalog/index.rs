use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    idx_id: u32,
    idx_name: String,
    idx_type: u8,
    idx_file: String,
    idx_tuples: u64,
    idx_columns: Vec<u16>,
    idx_is_primary: bool,
}