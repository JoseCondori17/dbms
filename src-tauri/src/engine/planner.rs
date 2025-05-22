use crate::catalog::catalog_manager::CatalogManager;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::sql::planner::SqlToRel;

pub fn create_planner_logic(statement: Statement, catalog: &CatalogManager) -> LogicalPlan {
    let planner = SqlToRel::new(catalog);
    planner.sql_statement_to_plan(statement).unwrap()
}

pub fn genera_logic_plans(statements: Vec<Statement>, catalog: &CatalogManager) -> Vec<LogicalPlan> {
    let mut plans = Vec::new();
    for statement in statements {
        let plan = create_planner_logic(statement, catalog);
        plans.push(plan);
    }
    plans
}