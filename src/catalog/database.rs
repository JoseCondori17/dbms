use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    id: u32,
    name: String,
    schemas: HashMap<String, u32>, // ref u32: table id
    created_at: DateTime<Utc>,
}

impl Database {
    pub fn new(id: u32, name: String) -> Self {
        Self {
            id,
            name,
            schemas: HashMap::new(),
            created_at: Utc::now(),
        }
    }
    // GETTERS
    pub fn get_id(&self) -> u32 {
        self.id
    }
    pub fn get_name(&self) -> &str {
        &self.name
    }
    pub fn get_schemas(&self) -> &HashMap<String, u32> {
        &self.schemas
    }
    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
    // METHODS
    pub fn add_schema(&mut self, name: String, id: u32) {
        self.schemas.insert(name, id);
    }
    pub fn delete_schema(&mut self, name: String, id: u32) {
        self.schemas.remove(&name);
    }
    pub fn get_schema_id(&self, name: &str) -> Option<&u32> {
        self.schemas.get(name)
    }
}   