use std::path::PathBuf;
use crate::catalog::column::Column;
use crate::catalog::constraint::Constraint;
pub struct Table {
    id: u32,
    name: String,
    schema_id: u32,
    columns: Vec<Column>,
    primary_key: Option<Vec<String>>,
    //constrains: Vec<Constraint>,
    heap_file_path: PathBuf
}