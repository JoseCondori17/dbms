use std::fs::{remove_dir_all, remove_file, create_dir, OpenOptions, File};
use serde::{Deserialize, Serialize};
use std::io::{Result};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
pub struct FileManager {
    base_path: PathBuf,
}

impl FileManager {
    pub fn new(base_path: &Path) -> Self {
        Self { base_path: base_path.to_path_buf() }
    }

    pub fn create_file(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
    }
    pub fn open_file(&self, path: &Path, read_only: bool) -> Result<File> {
        if read_only {
            OpenOptions::new()
                .read(true)
                .create(false)
                .open(path)
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(path)
        }
    }
    pub fn delete_file(&self, path: &Path) -> Result<()> {
        remove_file(path)?;
        Ok(())
    }
    pub fn create_directory(&self, path: &Path) -> Result<()> {
        create_dir(path)?;
        Ok(())
    }
    pub fn delete_directory(&self, path: &Path) -> Result<()> {
        remove_dir_all(path)?;
        Ok(())
    }
    pub fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.base_path.join(path);
        full_path.exists()
    }
}