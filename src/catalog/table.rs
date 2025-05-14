use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use crate::catalog::column::Column;

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    rel_id: u32,
    tab_name: String,
    tab_schema_id: u32,
    tab_columns: Vec<Column>,
    //tab_constrains: Vec<Constraint>,
    tab_primary_key: u8,
    tab_file_path: PathBuf
}