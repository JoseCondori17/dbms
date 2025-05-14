use sqlparser::ast::{Select, SetExpr, Statement};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use crate::catalog::schema::Schema;

pub enum LogicalPlan {
    Scan {
        tab_name: String,
        schema: Schema,
    },
    Filter {
        predicate: String,
        input: Box<LogicalPlan>,
    },
    Projection {
        columns: Vec<String>,
        input: Box<LogicalPlan>,
    },
}