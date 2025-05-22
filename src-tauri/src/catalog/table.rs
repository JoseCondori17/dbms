use std::sync::Arc;
use serde::{Deserialize, Serialize};
use datafusion::arrow::datatypes::{Field, Schema as ArrowShema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::TableSource;
use crate::catalog::column::Column;
use crate::catalog::index::Index;

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    tab_id: u32,
    tab_name: String,
    tab_namespace: u32, // schema id
    tab_tuples: u32, // quantity of record
    tab_pages: u32, // nums of pages
    tab_page_size: u32, // size of page
    tab_columns: Vec<Column>,
    tab_indexes: Vec<Index>,
}

impl Table {
    pub fn new(
        tab_id: u32,
        tab_name: String,
        tab_namespace: u32,
        tab_tuples: u32,
        tab_pages: u32,
        tab_page_size: u32,
        tab_columns: Vec<Column>,
        tab_indexes: Vec<Index>,
    ) -> Self {
        Table {
            tab_id,
            tab_name,
            tab_namespace,
            tab_tuples,
            tab_pages,
            tab_page_size,
            tab_columns,
            tab_indexes,
        }
    }
    pub fn get_tab_id(&self) -> u32 {
        self.tab_id
    }
    pub fn get_tab_name(&self) -> String {
        self.tab_name.clone()
    }
    pub fn get_tab_namespace(&self) -> u32 {
        self.tab_namespace
    }
    pub fn get_tab_tuples(&self) -> u32 {
        self.tab_tuples
    }
    pub fn get_tab_pages(&self) -> u32 {
        self.tab_pages
    }
    pub fn get_tab_page_size(&self) -> u32 {
        self.tab_page_size
    }
    pub fn get_tab_columns(&self) -> &Vec<Column> {
        self.tab_columns.as_ref()
    }
    pub fn get_tab_indexes(&self) -> Vec<Index> {
        self.tab_indexes.clone()
    }

    pub fn add_column(&mut self, column: Column) {
        self.tab_columns.push(column);
    }
    pub fn add_index(&mut self, index: Index) {
        self.tab_indexes.push(index);
    }
    
    // for physical plan
    pub fn to_arrow_schema(&self) -> Arc<ArrowShema> {
        // https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
        let fields: Vec<Field> = self.tab_columns
            .iter()
            .map(|c| c.to_arrow_field())
            .collect();
        Arc::new(ArrowShema::new(fields))
    }
    pub fn to_df_schema(&self) -> DFSchema {
        // https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
        DFSchema::try_from_qualified_schema(
            &self.tab_name,
            &self.to_arrow_schema()
        ).unwrap()
    }
}

// for physical plan
impl TableSource for Table {
    // https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.TableSource.html
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> Arc<ArrowShema> {
        self.to_arrow_schema()
    }
}