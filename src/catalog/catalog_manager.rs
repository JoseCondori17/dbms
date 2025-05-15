use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::path::{Path, PathBuf};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use datafusion::config::ConfigOptions;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as ResultFusion;
use datafusion::common::TableReference;
use datafusion::logical_expr::planner::ContextProvider;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use crate::catalog::column::Column;
use crate::catalog::database::Database;
use crate::catalog::index::Index;
use crate::catalog::schema::Schema;
use crate::storage::disk::file_manager::FileManager;
use crate::catalog::table::Table;
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
    pub fn load_catalog(&mut self) -> Result<(), CatalogError> {
        let global_catalog: GlobalCatalog = self.file_manager.read_data(self.system_catalog_path.as_path()).unwrap();
        self.global_catalog = global_catalog;
        Ok(())
    }
    pub fn save_catalog(&self) -> Result<(), CatalogError> {
        let catalog_path = Path::new(r"system\catalog.dat");
        if !self.file_manager.path_exists(catalog_path) {
            self.file_manager.create_file(self.system_catalog_path.as_path())?;
        }
        self.file_manager.write_data(&self.global_catalog, self.system_catalog_path.as_path());
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
        self.save_catalog()?;
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
        let database_id = database.get_id();
        database.add_schema(schema_name.to_string(), schema_id);
        // update database - metadata
        let path_db_meta = self.path_builder.database_meta(db_name);
        self.file_manager.write_data(&database, path_db_meta.as_path());

        // write schema - metadata
        let schema = Schema::new(
            schema_id, 
            schema_name.to_string(), 
            database_id
        );
        self.file_manager.write_data(&schema, path_sh_meta.as_path());

        // save database - catalog data
        self.save_catalog()?;
        Ok(())
    }
    pub fn create_table(&mut self, 
                        db_name: &str, 
                        schema_name: &str, 
                        table_name: &str, 
                        columns: Vec<Column>, 
                        indexes: Vec<Index>
    ) -> Result<(), CatalogError> {
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
        let table_id = self.generate_table_id(db_name, schema_name);
        let path_sh_meta = self.path_builder.schema_meta(db_name, schema_name);
        let mut schema_meta: Schema = self.file_manager.read_data(path_sh_meta.as_path()).unwrap();
        schema_meta.add_table(table_name.to_string(), table_id);
        self.file_manager.write_data(&schema_meta, path_sh_meta.as_path());
        // write table header - metadata
        let table_name = table_name.to_string();
        let schema_id = schema_meta.get_id();
        let tuples = 0;
        let pages = 1;
        let page_size = 8192;
        let table = Table::new(
            table_id,
            table_name,
            schema_id,
            tuples,
            pages,
            page_size,
            columns,
            indexes
        );
        self.file_manager.write_data(&table, path_table_header.as_path());
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
    pub fn get_databases(&self) -> Vec<&Database> {
        self.global_catalog.databases.values().collect()
    }
    pub fn get_database(&self, db_name: &str) -> Option<&Database> {
        self.global_catalog.databases.get(db_name)
    }
    pub fn get_database_mut(&mut self, db_name: &str) -> Option<&mut Database> {
        self.global_catalog.databases.get_mut(db_name)
    }
    pub fn get_database_names(&self) -> Vec<&str> {
        self.global_catalog.databases.keys().map(|x| x.as_str()).collect()
    }
    pub fn get_schemas(&self, db_name: &str) -> &HashMap<String, u32> {
        let database = self.global_catalog.databases.get(db_name).unwrap();
        database.get_schemas()
    }
    pub fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<Schema> {
        let path_sh_meta = self.path_builder.schema_meta(db_name, schema_name);
        let schema_meta= self.file_manager.read_data(path_sh_meta.as_path()).unwrap();
        Some(schema_meta)
    }
    pub fn get_tables(&self, db_name: &str, schema_name: &str) -> Vec<Table> {
        let database = self.global_catalog.databases.get(db_name).unwrap();
        let schema = database.get_schemas();
        let mut tables: Vec<Table> = Vec::new();
        for table_name in schema.keys() {
            let path_tab_meta = self.path_builder.table_header(db_name, schema_name, table_name);
            let table: Table = self.file_manager.read_data(path_tab_meta.as_path()).unwrap();
            tables.push(table);
        }
        tables
    }

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
        let mut max_id = 0;
        let path_schema = self.path_builder.schema_meta(db_name, schema_name);

        let mut buffer: Vec<u8> = Vec::new();
        let schema_meta: Schema = self.file_manager.read_data(path_schema.as_path()).unwrap();
        for table_id in schema_meta.get_tables().values() {
            if *table_id > max_id {
                max_id = *table_id;
            }
        }
        max_id + 1
    }
    fn generate_function_id(&self, db_name: &str, schema_name: &str) -> u32 {
        let mut max_id = 0;
        let path_schema = self.path_builder.schema_meta(db_name, schema_name);

        let mut buffer: Vec<u8> = Vec::new();
        let schema_meta: Schema = self.file_manager.read_data(path_schema.as_path()).unwrap();
        for table_id in schema_meta.get_functions().values() {
            if *table_id > max_id {
                max_id = *table_id;
            }
        }
        max_id + 1
    }
}

impl ContextProvider for CatalogManager {
    fn get_table_source(&self, name: TableReference) -> ResultFusion<Arc<dyn TableSource>> {
        let (db_name, schema_name, table_name) = match name {
            TableReference::Full { catalog, schema, table } => {
                // format database.schema.table
                (catalog, schema, table)
            },
            TableReference::Bare { .. }
            | TableReference::Partial { .. } => panic!("Unsupported table reference: {:?}", name),
        };
        let table_path = self.path_builder.table_header(&db_name, &schema_name, &table_name);
        println!("{:?}", table_path);
        let table_header: Table = self.file_manager.read_data(table_path.as_path()).unwrap();
        Ok(Arc::new(table_header))
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        None
    }
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        static CONFIG: OnceLock<ConfigOptions> = OnceLock::new();
        CONFIG.get_or_init(|| ConfigOptions::new())
    }
    fn udf_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for db in self.global_catalog.databases.values() {
            for schema_name in db.get_schemas().keys() {
                if let Some(schema) = self.get_schema(db.get_name(), schema_name) {
                    names.extend(schema.get_functions().keys().cloned());
                }
            }
        }
        names
    }
    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }
    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
}