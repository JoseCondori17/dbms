use std::fmt::Display;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement, SetExpr, ColumnDef, Ident, AssignmentTarget, Assignment, Expr, Value};
use sqlparser::dialect::PostgreSqlDialect;

#[warn(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum StatementType {
    DML,
    DDL,
    OTHER
}

pub fn get_statements(query: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, query)
}

pub fn get_columns(stmt: Statement) -> Option<Vec<ColumnDef>> {
    match stmt {
        Statement::CreateTable(stmt) => Some(stmt.columns.clone()),
        _ => None,
    }
}

pub fn get_table_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::CreateTable(create_table) => Some(create_table.name.to_string()),
        Statement::Query(query) => {
            match *query.body {
                SetExpr::Select(ref select_table) => Some(select_table.from.first().unwrap().to_string()),
                _ => None,
            }
        },
        Statement::Insert(insert_table) => Some(insert_table.table.to_string()),
        _ => None
    }
}

pub fn get_statement_type(stmt: &Statement) -> Option<StatementType> {
    match stmt {
        Statement::CreateTable { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. }
        | Statement::CreateView { .. }
        | Statement::CreateIndex { .. }
        | Statement::Truncate { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateDatabase { .. } => Some(StatementType::DDL),

        Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::Query { .. } => Some(StatementType::DML),

        _ => Some(StatementType::OTHER)
    }
}

pub fn get_values_selected(stmt: &Statement) -> Option<Vec<Ident>> {
    match stmt {
        Statement::Insert(stmt) => Some(stmt.columns.clone()),
        Statement::Update { assignments, .. } => {
            if assignments.is_empty() {
                return None
            }
            let assignment = assignments.first()?.value.clone();
            match assignment {
                Expr::CompoundIdentifier(identifier) => Some(identifier),
                _ => None
            }
        },
        _ => None,
    }
}