use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    db_id: u32,
    db_name: String,
    db_schemas: HashMap<String, u32>, // ref u32: table id
    db_created_at: DateTime<Utc>,
}

impl Database {
    pub fn new(id: u32, name: String) -> Self {
        Self {
            db_id: id,
            db_name: name,
            db_schemas: HashMap::new(),
            db_created_at: Utc::now(),
        }
    }
    
    // GETTERS
    pub fn get_id(&self) -> u32 {
        self.db_id
    }
    pub fn get_name(&self) -> &str {
        &self.db_name
    }
    pub fn get_schemas(&self) -> &HashMap<String, u32> {
        &self.db_schemas
    }
    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.db_created_at
    }
    
    // METHODS
    pub fn add_schema(&mut self, name: String, id: u32) {
        self.db_schemas.insert(name, id);
    }
    pub fn delete_schema(&mut self, name: String, id: u32) {
        self.db_schemas.remove(&name);
    }
    pub fn get_schema_id(&self, name: &str) -> Option<&u32> {
        self.db_schemas.get(name)
    }
}   