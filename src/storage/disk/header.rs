use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum IndexType {
    SEQUENTIAL = 1,
    AVL = 2,
    ISAM = 3,
    EXTENDIBLE_HASHING = 4,
    BPLUS_TREE = 5,
    R_TREE = 6
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexInfo {
    name: String,
    column_count: Vec<u16>,
    index_type: u8,
    file_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnInfo {
    name: String,
    data_type: u8,
    nullable: bool,
    offset: u16,
    size: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableHeader {
    version: u32,
    column_count: u16,
    columns: Vec<ColumnInfo>,
    row_count: u64,
    page_size: u32,
    primary_key_columns: Vec<u16>,
    //first_free_page: Option<u32>,
    index_info: Vec<IndexInfo>,
}