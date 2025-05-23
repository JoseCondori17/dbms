from sqlglot.dialects.postgres import Postgres
from sqlglot.expressions import (
    Expression, 
    Table, 
    ColumnDef, 
    Identifier, 
    DataType, 
    Literal,
    Tuple,
    Var,
    IndexParameters,
    DefaultColumnConstraint, 
    NotNullColumnConstraint
)
from sqlglot import parse

from catalog.column import Column
from models.enum.data_type_enum import DataTypeTag, DataTypeSize

def parser_sql(query: str) -> list[Expression]:
    return parse(query, dialect=Postgres)

def get_name(expr: Expression) -> str:
    return expr.this.name

def get_table_name(expr: Expression):
    table = expr.find(Table)
    return table.name if table else None

def get_table_alias(expr: Expression):
    table = expr.find(Table)
    return table.alias if table else None

def get_table_catalog(expr: Expression):
    table = expr.find(Table)
    return table.catalog if table else None

def get_table_schema(expr: Expression):
    table = expr.find(Table)
    return table.db if table else None

def get_columns(expr: Expression) -> list[Column]:
    columns = []
    for column in expr.find_all(ColumnDef):
        identifier = column.find(Identifier)
        if not identifier:
            continue
        att_name = identifier.name
        
        data_type_param = column.find(DataType)
        type = data_type_param.this.name
        data_type = DataTypeTag[type]
        if not data_type:
            continue

        data_size = column.find(Literal).output_name if column.find(Literal) else None

        att_len = int(data_size) if data_size else DataTypeSize[type].value

        att_not_null = column.find(NotNullColumnConstraint) is not None
        
        att_has_def = column.find(DefaultColumnConstraint) is not None

        columns.append(
            Column(
                att_name=att_name,
                att_type_id=data_type.value,
                att_len=att_len,
                att_not_null=att_not_null,
                att_has_def=att_has_def,
            )
        )
    return columns

def get_values(expr: Expression) -> list[Tuple]:
    return expr.find_all(Tuple)

def to_tuple(expr: Expression) -> tuple:
    values = []
    for value in expr.find_all(Literal):
        if value.this:
            values.append(value.to_py())
    return tuple(values)

def get_index_type(expr: Expression) -> str:
    params = expr.find(IndexParameters)
    index = params.find(Var)
    return index.name if index else None

def get_column_name(expr: Expression) -> str:
    params = expr.find(IndexParameters)
    identifier = params.find(Identifier)
    return identifier.name if identifier else None