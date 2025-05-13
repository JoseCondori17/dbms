mod catalog;
mod storage;
mod engine;
mod types;
mod utils;
mod query;

use pyo3::prelude::*;
use pyo3::types::{PyList};


fn main() -> PyResult<()> {
    let selectQuery = "SELECT name, age as lider FROM users";
    let dropTableQuery = "DROP TABLE products";
    let dropDatabaseQuery = "DROP DATABASE inventory";
    let createTableQuery = "CREATE TABLE employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))";
    let updateQuery = "UPDATE user SET name = 'jose', city='lima'";
    let dropSchemaQuery = "DROP SCHEMA hr CASCADE";

    Python::with_gil(|py| {
        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1("append", ("./src/query",))?;


        let parser_module = py.import("parser_python")?;

        let binding = parser_module
            .getattr("get_statements")?
            .call1((createTableQuery,))?;

        let statements = binding
            .downcast::<PyList>()?;

        for stmt in statements.iter() {
            let table_name = parser_module
                .getattr("get_table_name")?
                .call1((stmt,))?
                .extract::<String>()?;
            println!("{:?}", table_name);
        }

        Ok(())
    })

}
