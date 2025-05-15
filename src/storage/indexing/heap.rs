use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use crate::catalog::table::Table;

pub struct HeapFile {
    file_path: PathBuf,
    header: Table,
    file: File,
}

impl HeapFile {
    pub fn new(file_path: PathBuf, header: Table, file: File) -> Self {
        Self {
            file_path,
            header,
            file,
        }
    }
    pub fn insert_row(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.file.write_all(data)?;
        Ok(())
    }
    pub fn get_row(&mut self, id: &str) {}
    pub fn update_row(&mut self, id: &str, data: &[u8]) {}
    pub fn delete_row(&mut self, id: &str) {}
}

