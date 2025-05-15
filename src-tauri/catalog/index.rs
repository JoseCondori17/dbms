use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Index {
    idx_id: u32,
    idx_type: u8,
    idx_name: String,
    idx_file: PathBuf,
    idx_tuples: u64,
    idx_columns: Vec<u16>, // column position
    idx_is_primary: bool,
}

impl Index {
    pub fn get_idx_id(&self) -> u32 {
        self.idx_id
    }
    pub fn get_idx_name(&self) -> String {
        self.idx_name.clone()
    }
    pub fn get_idx_type(&self) -> u8 {
        self.idx_type
    }
    pub fn get_idx_file(&self) -> PathBuf {
        self.idx_file.clone()
    }
    pub fn get_idx_tuples(&self) -> u64 {
        self.idx_tuples
    }
    pub fn get_idx_columns(&self) -> Vec<u16> {
        self.idx_columns.clone()
    }
    pub fn get_idx_is_primary(&self) -> bool {
        self.idx_is_primary
    }
}