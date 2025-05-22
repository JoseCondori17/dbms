use std::sync::Arc;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan};
use crate::catalog::catalog_manager::CatalogManager;
use crate::catalog::column::Column;
use crate::storage::indexing::heap::HeapFile;
use crate::types::data_types::{DataValue};

pub struct Insert {
    pub db_name: String,
    pub sch_name: String,
    pub tab_name: String,
    pub plan: LogicalPlan
}

impl Insert {
    pub fn new(
        db_name: String,
        sch_name: String,
        tab_name: String,
        plan: LogicalPlan
    ) -> Self {
        Insert {
            db_name,
            sch_name,
            tab_name,
            plan
        }
    }
    pub fn execute(&self, catalog: &Arc<CatalogManager>) -> Result<(), String> {
        let table_info = catalog.get_table(&self.db_name, &self.sch_name, &self.tab_name)
            .ok_or_else(|| format!("Table not found: {}", self.tab_name))?;
        let columns = table_info.get_tab_columns().clone();
        
        let path_builder = catalog.get_path_builder();
        let file_path = path_builder.table_data(&self.db_name, &self.sch_name, &self.tab_name);
        let heap_file = catalog
            .get_file_manager()
            .open_file_append(&file_path)
            .map_err(|e| format!("Error opening heap file: {}", e))?;
        let mut heap_file = HeapFile::new(table_info, heap_file);
        self.plan.apply(|node| {
            if let LogicalPlan::Values(v) = node {
                for express in &v.values {
                    let row: Vec<DataValue> = self.expr_to_data_value(&express, &columns);
                    heap_file.insert_row(row).unwrap();
                };
            }
            Ok(TreeNodeRecursion::Continue)
        }).unwrap();
        heap_file.finalize()?;
        Ok(())
    }

    fn expr_to_data_value(&self, expr: &Vec<Expr>, col: &Vec<Column>) -> Vec<DataValue> {
        let mut row: Vec<DataValue> = Vec::new();
        for (expr, col) in expr.iter().zip(col.iter()) {
            if let Expr::Cast(cast) = expr {
                if let Expr::Literal(value) = cast.expr.as_ref() {
                    let value = value.to_string();
                    row.push(match col.get_att_type_id() {
                        1 => DataValue::SmallInt(value.parse().unwrap()),
                        2 => DataValue::Int(value.parse().unwrap()),
                        3 => DataValue::BigInt(value.parse().unwrap()),
                        4 => DataValue::Double(value.parse().unwrap()),
                        5 => DataValue::Char(value.into_bytes()),
                        6 => DataValue::VarChar(value.into_bytes()),
                        7 => DataValue::Bool(value.parse().unwrap()),
                        8 => DataValue::Uuid(
                            uuid::Uuid::parse_str(&value)
                                .unwrap()
                                .into_bytes(),
                        ),
                        9 => DataValue::Date(value.parse().unwrap()),
                        10 => DataValue::Time(value.parse().unwrap()),
                        11 => DataValue::Timestamp(value.parse().unwrap()),
                        _ => panic!("Invalid type id"),
                    });
                }
            }
        }
        row
    }
}