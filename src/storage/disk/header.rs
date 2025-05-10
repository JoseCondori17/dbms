pub struct IndexInfo {
    name: String,
    column_count: Vec<u16>,
    index_type: u8,
    file_name: String,
}
pub struct TableHeader {
    version: u32,
    column_count: u16,
    columns: Vec<ColumnInfo>,
    row_count: u64,
    page_size: u32,
    first_free_page: Option<u32>,
    primary_key_columns: Vec<u16>,
    index_info: Vec<IndexInfo>,
}
pub struct ColumnInfo {
    name: String,
    data_type: u8,
    nullable: bool,
    offset: u16,
    size: u16,
}