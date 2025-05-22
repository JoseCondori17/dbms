use std::sync::Arc;
use datafusion::logical_expr::LogicalPlan;
use crate::catalog::catalog_manager::CatalogManager;
use super::operators::insert::Insert;

pub struct Executor {
    catalog: Arc<CatalogManager>,
}
impl Executor {
    pub fn new(catalog: Arc<CatalogManager>) -> Self {
        Self { catalog }
    }

    pub fn execute_plan(&self, plan: &LogicalPlan) -> Result<(), String> {
        match plan {
            LogicalPlan::Dml(insert) => {
                let db_name = insert.table_name.catalog().ok_or_else(||format!("Database not found for table: {:?}", insert.table_name))?;
                let sch_name = insert.table_name.schema().ok_or_else(||format!("Schema not found for table: {:?}", insert.table_name))?;
                let tab_name = insert.table_name.table();
                let insert_op = Insert::new(db_name.to_string(), sch_name.to_string(), tab_name.to_string(), plan.clone());
                insert_op.execute(&self.catalog)?;
                Ok(())
            },
            _ => Err(format!("Planner not supported: {:?}", plan)),
        }
    }
}