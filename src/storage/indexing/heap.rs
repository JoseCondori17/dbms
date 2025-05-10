use std::fs::File;
use std::path::PathBuf;
use crate::storage::disk::header::TableHeader;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct HeapFile {
    file_path: PathBuf,
    header: TableHeader,
    file: File,
}

impl HeapFile {
    pub fn new() {}
    pub fn insert_row(&mut self, data: &[u8]) {
        
    }
    pub fn get_row(&mut self, id: &str) {}
    pub fn update_row(&mut self, id: &str, data: &[u8]) {}
    pub fn delete_row(&mut self, id: &str) {}
}

