mod catalog;
mod storage;
mod engine;
mod types;
mod utils;
mod query;

use std::path::Path;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::planner::SqlToRel;
use crate::catalog::catalog_manager::CatalogManager;
use datafusion::common::display::StringifiedPlan;

fn main() {
    let selectQuery = "SELECT name, age as lider FROM users";
    let dropTableQuery = "DROP TABLE products";
    let dropDatabaseQuery = "DROP DATABASE inventory";
    let createTableQuery = "CREATE TABLE employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))";
    let updateQuery = "UPDATE user SET name = 'jose', city='lima'";
    let drop_schema_query = "DROP SCHEMA hr CASCADE";

    let catalog_manager = CatalogManager::new(Path::new(r".\src\data")).unwrap();
    //let scheme = catalog_manager.get_schema("crunchy", "store").unwrap();
    let dialect = PostgreSqlDialect {};
    let sql = "SELECT id, name FROM crunchy.store.user WHERE id > 100";
    let stmt = Parser::parse_sql(&dialect, createTableQuery).unwrap()[0].clone();

    let planner = SqlToRel::new(&catalog_manager);
    let logical_plan = planner.sql_statement_to_plan(stmt).expect("Failed to plan query");
    println!("{}", logical_plan.display_pg_json())
}