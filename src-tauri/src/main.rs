mod catalog;
mod storage;
mod engine;
mod types;
mod utils;
mod query;

use std::path::Path;
use std::sync::Arc;
use datafusion::common::tree_node::{TreeNode, TreeNodeContainer, TreeNodeRecursion};
use datafusion::logical_expr::{DmlStatement, LogicalPlan, WriteOp};
use datafusion::logical_expr::LogicalPlan::{Dml, Projection};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::planner::SqlToRel;

use crate::catalog::catalog_manager::CatalogManager;
use crate::catalog::column::Column;
use crate::engine::executor::Executor;
use crate::storage::indexing::heap::HeapFile;

fn create_table_test(){
    let mut catalog_manager = CatalogManager::new(Path::new(r".\src\data")).unwrap();
    let columns: Vec<Column> = vec![
        Column::new("id".to_string(),2, 4, true, false),
        Column::new("name".to_string(),6, 45, true, false),
        Column::new("last_name".to_string(),6, 45, true, false)
    ];
    catalog_manager.create_table("crunchy", "store", "user", columns, vec![]).unwrap();
    let table = catalog_manager.get_table("crunchy", "store", "user").unwrap();
    println!("{:?}", table);
}

fn executor_insert_into_test() {
    let insert_query = "INSERT INTO crunchy.store.user (id, name, last_name) VALUES (1, 'Jose Condori', 'Condori'), (2, 'María García', 'Palomino'), (3, 'Carlos López', 'Juan Pérez'), (4, 'Ana Martínez', 'Juan Pérez'), (5, 'Luisa Fernández', 'Juan Pérez');";
    let catalog_manager = CatalogManager::new(Path::new(r".\src\data")).unwrap();
    let catalog_manager_arc = Arc::new(catalog_manager);

    let dialect = PostgreSqlDialect {};
    let stmt = Parser::parse_sql(&dialect, insert_query).unwrap()[0].clone();

    let planner = SqlToRel::new(&*catalog_manager_arc);
    let logical_plan = planner.sql_statement_to_plan(stmt).expect("Failed to plan query");
    let table = catalog_manager_arc.get_table("crunchy", "store", "user").unwrap();
    let executor = Executor::new(catalog_manager_arc);
    executor.execute_plan(&logical_plan).unwrap();
}

fn main() {
    let selectQuery = "SELECT name, age as lider FROM users";
    let dropTableQuery = "DROP TABLE products";
    let dropDatabaseQuery = "DROP DATABASE inventory";
    let createTableQuery = "CREATE TABLE crunchy.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))";
    let updateQuery = "UPDATE user SET name = 'jose', city='lima'";
    let drop_schema_query = "DROP SCHEMA hr CASCADE";
    let insert_query = "INSERT INTO crunchy.store.user (id, name, last_name) VALUES (1, 'Jose Condori', 'Condori'), (2, 'María García', 'Palomino'), (3, 'Carlos López', 'Juan Pérez'), (4, 'Ana Martínez', 'Juan Pérez'), (5, 'Luisa Fernández', 'Juan Pérez');";
    
    let catalog_manager = CatalogManager::new(Path::new(r".\src\data")).unwrap();

    let path = catalog_manager.get_path_builder().table_data("crunchy", "store", "user");
    let file = catalog_manager.get_file_manager().open_file(path.as_path(), true).unwrap();
    let table_info = catalog_manager.get_table("crunchy", "store", "user").unwrap();
    let mut heap_file = HeapFile::new(table_info, file);
    let strs = heap_file.get_all_rows().unwrap();
    println!("{:?}", strs);
    
    //println!("{}", logical_plan.display_indent().to_string());
}

/*
logical_plan.apply(|x| {
        println!("element {:?}", x);
        println!("____________");
        Ok(TreeNodeRecursion::Continue)
    }).unwrap();
*/