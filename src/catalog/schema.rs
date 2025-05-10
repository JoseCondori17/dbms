use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    id: u32,
    name: String,
    database_id: u32,
    tables: HashMap<String, u32>, // ref u32: table id
    functions: HashMap<String, u32>, // ref u32: function id
}

impl Schema {
    pub fn new(id: u32, name: String, database_id: u32) -> Self {
        Self {
            id,
            name,
            database_id,
            tables: HashMap::new(),
            functions: HashMap::new(),
        }   
    }
    // GETTERS
    pub fn get_id(&self) -> u32 {
        self.id
    }
    pub fn get_name(&self) -> String {
        self.name.clone()
    }
    pub fn get_database_id(&self) -> u32 {
        self.database_id
    }
    pub fn get_tables(&self) -> HashMap<String, u32> {
        self.tables.clone()
    }
    pub fn get_functions(&self) -> HashMap<String, u32> {
        self.functions.clone()
    }
    
    // METHODS
    pub fn add_table(&mut self, name: String, id: u32) {
        self.tables.insert(name, id);
    }
    pub fn delete_table(&mut self, name: String) {
        self.tables.remove(&name);
    }
    pub fn add_function(&mut self, name: String, id: u32) {
        self.functions.insert(name, id);
    }
    pub fn delete_function(&mut self, name: String) {
        self.functions.remove(&name);
    }
}