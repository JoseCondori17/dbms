use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    sch_id: u32,
    sch_name: String,
    sch_db_id: u32,
    sch_tables: HashMap<String, u32>, // ref u32: table id
    sch_functions: HashMap<String, u32>, // ref u32: function id
}

impl Schema {
    pub fn new(id: u32, name: String, database_id: u32) -> Self {
        Self {
            sch_id: id,
            sch_name: name,
            sch_db_id: database_id,
            sch_tables: HashMap::new(),
            sch_functions: HashMap::new(),
        }   
    }
    // GETTERS
    pub fn get_id(&self) -> u32 {
        self.sch_id
    }
    pub fn get_name(&self) -> String {
        self.sch_name.clone()
    }
    pub fn get_database_id(&self) -> u32 {
        self.sch_db_id
    }
    pub fn get_tables(&self) -> HashMap<String, u32> {
        self.sch_tables.clone()
    }
    pub fn get_functions(&self) -> HashMap<String, u32> {
        self.sch_functions.clone()
    }
    
    // METHODS
    pub fn add_table(&mut self, name: String, id: u32) {
        self.sch_tables.insert(name, id);
    }
    pub fn delete_table(&mut self, name: String) {
        self.sch_tables.remove(&name);
    }
    pub fn add_function(&mut self, name: String, id: u32) {
        self.sch_functions.insert(name, id);
    }
    pub fn delete_function(&mut self, name: String) {
        self.sch_functions.remove(&name);
    }
}