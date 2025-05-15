from sqlglot import parse
from sqlglot.expressions import Expression, ColumnDef, DML, DDL, Table, Identifier, Column, Constraint, \
    ColumnConstraint, ColumnConstraintKind
from sqlglot.dialects.postgres import Postgres

def get_statements(query: str) -> list[Expression]:
    return parse(query, dialect=Postgres)

def get_columns(stmt: Expression) -> list[dict[str, str]]:
    columns = []
    for column in stmt.find_all(ColumnDef):
        column_dict = {
            "name": column.find(Identifier).name,
            "type": column.find(Identifier).args.get("this"),
            "constraints": column.find(ColumnConstraintKind)
        }
        columns.append(column_dict)
    return columns

def get_table_name(stmt: Expression) -> str:
    return stmt.find(Table).name.lower()

def get_statement_type(stmt: Expression):
    if isinstance(stmt, DML):
        return "DML"
    elif isinstance(stmt, DDL):
        return "DDL"
    return ""

def get_statement_name(stmt: Expression) -> str:
    return stmt.__class__.__name__.upper()

def get_statement_kind_name(stmt: Expression) -> str:
    return stmt.args.get("kind")
def get_values_selected(stmt: Expression) -> list[str]:
    values = []
    for column in stmt.find_all(Column):
        values.append(column.name)
    return values