use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use bincode;
use chrono::{DateTime, Utc};
use crate::catalog::database::Database;
use crate::catalog::schema::Schema;
use crate::storage::disk::file_manager::FileManager;
use crate::storage::disk::path::PathBuilder;
use crate::utils::error::CatalogError;

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobalCatalog {
    pub databases: HashMap<String, Database>,
    pub version: String,
    pub created_at: DateTime<Utc>,
}

pub struct CatalogManager {
    system_catalog_path: PathBuf,
    file_manager: FileManager,
    path_builder: PathBuilder,
    global_catalog: GlobalCatalog,
}

impl CatalogManager {
    pub fn new(data_directory: &Path) -> Result<Self, CatalogError> {
        let catalog_path = Path::new(r"system\catalog.dat");
        let system_catalog_path = data_directory.join(catalog_path);
        let file_manager = FileManager::new(data_directory);
        let path_builder = PathBuilder::new(&data_directory.to_path_buf());

        if !file_manager.path_exists(Path::new("system")) {
            let path_system = path_builder.system_dir("system");
            file_manager.create_directory(path_system.as_path())?;
        }

        let global_catalog = GlobalCatalog {
            databases: HashMap::new(),
            version: "0.0.1".to_string(),
            created_at: Utc::now(),
        };
        
        let mut catalog_manager = Self {
            system_catalog_path,
            file_manager,
            path_builder,
            global_catalog,
        };

        let is_created = catalog_manager.file_manager.path_exists(catalog_path);
        if is_created {
            catalog_manager.load_catalog()?;
        } else {
            catalog_manager.save_catalog()?;
        }

        Ok(catalog_manager)
    }
    
    // SAVE GLOBAL DATA
    pub fn load_catalog(&mut self) -> Result<(), CatalogError>  {
        let mut file = self.file_manager.open_file(&self.system_catalog_path.clone(), true)?;
        let mut buffer: Vec<u8> = Vec::new();
        file.read_to_end(&mut buffer)?;
        let config = bincode::config::standard().with_little_endian();
        let (global_catalog, _): (GlobalCatalog, _) = bincode::serde::decode_from_slice(&buffer, config)?;
        self.global_catalog = global_catalog;
        Ok(())
    }
    pub fn save_catalog(& self) -> Result<(), CatalogError> {
        let catalog_path = Path::new(r"system\catalog.dat");
        if !self.file_manager.path_exists(catalog_path) {
            self.file_manager.create_file(self.system_catalog_path.as_path())?;
        }
        let config = bincode::config::standard().with_little_endian();
        let catalog_data: Vec<u8> = bincode::serde::encode_to_vec(&self.global_catalog, config)?;
        let mut file = self.file_manager.open_file(&self.system_catalog_path.clone(), false)?;
        file.write_all(&catalog_data)?;
        Ok(())
    }

    // CREATE FUNCTIONS
    pub fn create_database(&mut self, db_name: &str) -> Result<(), CatalogError> {
        let path_db = self.path_builder.database_dir(db_name);
        let path_db_meta = self.path_builder.database_meta(db_name);

        self.file_manager.create_directory(path_db.as_path())?;
        self.file_manager.create_file(path_db_meta.as_path())?;
        let db_id = self.generate_database_id();
        let database = Database::new(db_id, db_name.to_string());
        self.global_catalog.databases.insert(db_name.to_string(), database);
        self.save_catalog()?; // save database - catalog data
        Ok(())
    }
    pub fn create_schema(&mut self, db_name: &str, schema_name: &str) -> Result<(), CatalogError> {
        let path_sh = self.path_builder.schema_dir(db_name, schema_name);
        let path_sh_meta = self.path_builder.schema_meta(db_name, schema_name);

        if !self.file_manager.path_exists(path_sh.as_path()) {
            self.file_manager.create_directory(path_sh.as_path())?;
        }
        self.file_manager.create_file(path_sh_meta.as_path())?;
        let schema_id = self.generate_schema_id(db_name);

        let database = self.global_catalog.databases.get_mut(db_name).unwrap();
        database.add_schema(schema_name.to_string(), schema_id);
        let database_id = database.get_id();
        // update database - metadata
        let path_db_meta = self.path_builder.database_meta(db_name);
        let config = bincode::config::standard().with_little_endian();
        let database_meta = bincode::serde::encode_to_vec(&database, config)?;
        let mut file = self.file_manager.open_file(path_db_meta.as_path(), false)?;
        file.write_all(&database_meta)?;
        
        // write schema - metadata
        let schema = Schema::new(schema_id, schema_name.to_string(), database_id);
        let config = bincode::config::standard().with_little_endian();
        let schema_meta = bincode::serde::encode_to_vec(&schema, config)?;
        let mut file = self.file_manager.open_file(path_sh_meta.as_path(), false)?;
        file.write_all(&schema_meta)?;
        file.flush()?;
        
        // save database - catalog data
        self.save_catalog()?;
        Ok(())
    }
    pub fn create_table(&mut self, db_name: &str, schema_name: &str, table_name: &str) -> Result<(), CatalogError> {
        let path_table = self.path_builder.table_dir(db_name, schema_name, table_name);
        let path_table_data = self.path_builder.table_data(db_name, schema_name, table_name);
        let path_table_header = self.path_builder.table_header(db_name, schema_name, table_name);
        let path_table_idx_pk = self.path_builder.table_index(db_name, schema_name, table_name, "pk");

        if !self.file_manager.path_exists(path_table.as_path()) {
            self.file_manager.create_directory(path_table.as_path())?;
        }
        self.file_manager.create_file(path_table_data.as_path())?;
        self.file_manager.create_file(path_table_header.as_path())?;
        self.file_manager.create_file(path_table_idx_pk.as_path())?;

        // update schema - metadata
        let path_sh_meta = self.path_builder.schema_meta(db_name, schema_name);
        let mut file = self.file_manager.open_file(path_sh_meta.as_path(), true)?;
        let mut buffer: Vec<u8> = Vec::new();
        file.read_to_end(&mut buffer)?;
        let config = bincode::config::standard().with_little_endian();
        
        let (mut schema_meta, _) : (Schema, _) = bincode::serde::decode_from_slice(&buffer, config)?;
        schema_meta.add_table(table_name.to_string(), 1); // change
        // write table - metadata
        let schema_data = bincode::serde::encode_to_vec(schema_meta, config)?;
        let mut file = self.file_manager.open_file(path_sh_meta.as_path(), false)?;
        file.write_all(&schema_data)?;
        file.flush()?;
        Ok(())
    }
    pub fn create_index(&mut self, db_name: &str, schema_name: &str, table_name: &str, index_name: &str) -> std::io::Result<()> {
        let path_idx_name = self.path_builder.function_file(db_name, schema_name, table_name, index_name);
        self.file_manager.create_file(path_idx_name.as_path())?;
        Ok(())
    }
    pub fn create_function(&mut self, db_name: &str, schema_name: &str, table_name: &str, function_name: &str) -> Result<(), CatalogError> {
        let path_fn_name = self.path_builder.table_index(db_name, schema_name, table_name, function_name);
        self.file_manager.create_file(path_fn_name.as_path())?;
        Ok(())
    }

    // GETTERS
    pub fn get_databases(&self) {}
    pub fn get_database(&self, db_name: &str) -> Option<&Database> {
        self.global_catalog.databases.get(db_name)
    }
    pub fn get_database_mut(&mut self, db_name: &str) -> Option<&mut Database> {
        self.global_catalog.databases.get_mut(db_name)
    }
    pub fn get_database_name(&self) -> Vec<&str> {
        self.global_catalog.databases.keys().map(|x| x.as_str()).collect()
    }
    pub fn get_schemas(&self, db_name: &str) -> &HashMap<String, u32> {
        let database = self.global_catalog.databases.get(db_name).unwrap();
        database.get_schemas()
    }
    pub fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<Schema> {
        let path_sh_meta = self.path_builder.schema_meta(db_name, schema_name);
        let mut file = self.file_manager.open_file(path_sh_meta.as_path(), true).unwrap();
        let config = bincode::config::standard().with_little_endian();
        let mut buffer: Vec<u8> = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let (schema_meta, _) : (Schema, _) = bincode::serde::decode_from_slice(&buffer, config).unwrap();
        Some(schema_meta)
    }
    //pub fn get_tables(&self, db_name: &str, schema_name: &str) {}

    // HELP FUNCTIONS
    fn generate_database_id(&self) -> u32 {
        let mut max_id = 0;
        for db in self.global_catalog.databases.values() {
            if db.get_id() > max_id {
                max_id = db.get_id();
            }
        }
        max_id + 1
    }
    fn generate_schema_id(&self, db_name: &str) -> u32 {
        let mut max_id = 0;
        let database = self.global_catalog.databases.get(db_name).unwrap();

        for schema_id in database.get_schemas().values() {
            if *schema_id > max_id {
                max_id = *schema_id;
            }
        }
        max_id + 1
    }
    fn generate_table_id(&self, db_name: &str, schema_name: &str) -> u32 {
        let max_id = 0;
        max_id + 1
    }
    fn generate_function_id(&self, db_name: &str, schema_name: &str) -> u32 {
        let max_id = 0;
        max_id + 1
    }
}