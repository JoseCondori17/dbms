use serde::{Deserialize, Serialize};
use crate::catalog::column::Column;
use crate::catalog::index::Index;

#[derive(Serialize, Deserialize, Debug)]
pub struct TableHeader {
    rel_id: u32,
    rel_name: String,
    rel_namespace: u32, // schema id
    rel_type: Vec<Column>,
    rel_tuples: u32, // quantity of record
    rel_pages: u32, // nums of pages
    rel_page_size: u32, // size of page

    rel_indexes: Vec<Index>,
    
    primary_key_columns: Vec<u16>,
}