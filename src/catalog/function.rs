use std::path::PathBuf;

pub struct Function {
    fn_id: u32,
    fn_name: String,
    fn_type: String,
    fn_file_path: PathBuf,
}