use std::fmt::format;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PathBuilder {
    base_dir: PathBuf,
}

impl PathBuilder {
    pub fn new(base_dir: &PathBuf) -> Self {
        Self { base_dir: base_dir.clone() }
    }
    pub fn base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }
    pub fn system_dir(&self, system: &str) -> PathBuf {
        self.base_dir.join(system)
    }
    pub fn database_dir(&self, db_name: &str) -> PathBuf {
        self.base_dir.join(format!("db_{}", db_name))
    }
    pub fn database_meta(&self, db_name: &str) -> PathBuf {
        self.database_dir(db_name).join("meta.dat")
    }
    pub fn schema_dir(&self, db_name: &str, schema_name: &str) -> PathBuf {
        self.database_dir(db_name).join(format!("schema_{}", schema_name))
    }
    pub fn schema_meta(&self, db_name: &str, schema_name: &str) -> PathBuf {
        self.schema_dir(db_name, schema_name).join("meta.dat")
    }
    pub fn table_dir(&self, db_name: &str, schema_name: &str, table_name: &str) -> PathBuf {
        self.schema_dir(db_name, schema_name).join(format!("table_{}", table_name))
    }
    pub fn table_data(&self, db_name: &str, schema_name: &str, table_name: &str) -> PathBuf {
        self.table_dir(db_name, schema_name, table_name).join("data.dat")
    }
    pub fn table_header(&self, db_name: &str, schema_name: &str, table_name: &str) -> PathBuf {
        self.table_dir(db_name, schema_name, table_name).join("header.dat")
    }
    pub fn table_index(&self, db_name: &str, schema_name: &str, table_name: &str, property: &str) -> PathBuf {
        self.table_dir(db_name, schema_name, table_name).join(format!("idx_{}_{}.dat", property, table_name))
    }
    pub fn function_file(&self, db_name: &str, schema_name: &str, table_name: &str, function_name: &str) -> PathBuf {
        self.schema_dir(db_name, schema_name).join(format!("fn_{}_{}.dat", function_name, table_name))
    }
}