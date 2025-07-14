from sqlglot.dialects.postgres import Postgres
from sqlglot.expressions import (
    Expression, 
    Table, 
    ColumnDef, 
    Identifier, 
    DataType, 
    Literal,
    Boolean,
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
    # Pre-process the query to handle USING clauses that SQLglot doesn't support
    processed_query = preprocess_index_syntax(query)
    return parse(processed_query, dialect=Postgres)

def preprocess_index_syntax(query: str) -> str:
    """
    Pre-process CREATE INDEX statements to handle USING clauses that SQLglot doesn't support.
    This temporarily removes the USING clause and stores it for later extraction.
    """
    import re
    
    # Pattern to match CREATE INDEX with USING clause
    # Handle both USING type(column) and USING type formats
    pattern = r'(CREATE\s+INDEX\s+\w+\s+ON\s+[\w.]+\s*\([^)]+\))\s+USING\s+(\w+)(?:\([^)]+\))?'
    
    def replace_using_clause(match):
        # Extract the main CREATE INDEX part and the index type
        create_part = match.group(1)
        index_type = match.group(2).upper()
        
        # Store the index type in a comment that we can parse later
        return f"{create_part} /* INDEX_TYPE:{index_type} */"
    
    # Replace USING clauses with comments
    processed = re.sub(pattern, replace_using_clause, query, flags=re.IGNORECASE)
    
    return processed

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
    
    # Get all Literal and Boolean expressions from the tuple
    for value in expr.find_all(Literal):
        if value.this:
            values.append(value.to_py())
    
    for value in expr.find_all(Boolean):
        values.append(value.to_py())
    
    return tuple(values)

def get_identifier(expr: Expression):
    ident = expr.find(Identifier)
    return list(ident.args.values())[0]

def get_index_type(expr: Expression) -> str:
    params = expr.find(IndexParameters)
    if params:
        index = params.find(Var)
        if index:
            return index.name
    
    # Extract index type from preprocessed comment
    if hasattr(expr, 'sql') and expr.sql:
        sql_str = expr.sql()
        import re
        match = re.search(r'/\*\s*INDEX_TYPE:(\w+)\s*\*/', sql_str)
        if match:
            return match.group(1)
    
    # Fallback: try to extract from raw SQL if IndexParameters not found
    # This handles cases where SQLglot doesn't parse USING clause correctly
    if hasattr(expr, 'sql') and expr.sql:
        sql_str = expr.sql().upper()
        if 'USING HASH' in sql_str:
            return 'HASH'
        elif 'USING BTREE' in sql_str:
            return 'BTREE'
        elif 'USING ISAM' in sql_str:
            return 'ISAM'
        elif 'USING SEQUENTIAL' in sql_str:
            return 'SEQUENTIAL'
        elif 'USING RTREE' in sql_str:
            return 'RTREE'
    
    return None

def get_column_name(expr: Expression) -> str:
    params = expr.find(IndexParameters)
    if params:
        identifier = params.find(Identifier)
        if identifier:
            return identifier.name
    
    # Fallback: extract column name from the index expression
    # Look for the column specification in the CREATE INDEX statement
    if hasattr(expr, 'expressions') and expr.expressions:
        for exp in expr.expressions:
            if hasattr(exp, 'this') and hasattr(exp.this, 'name'):
                return exp.this.name
    
    return None

def get_copy_info(expr: Expression):
    table = expr.find(Table)
    filename = expr.find(Literal)
    
    if not table or not filename:
        raise ValueError("COPY: tabla o filename no reconocidos por el parser")

    return table.catalog, table.db, table.name, filename.name