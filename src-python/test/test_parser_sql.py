import pytest
from sqlglot.expressions import Expression
from query.parser_sql import (
    parser_sql,
)

@pytest.fixture
def simple_create_table():
    return """
    CREATE TABLE acm.public.users (
        id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN
    );
    """

@pytest.fixture
def complex_create_table():
    return """
    CREATE TABLE ecm.store.employees (
        employee_id BIGINT NOT NULL,
        full_name VARCHAR(100),
        salary DOUBLE PRECISION,
        department CHAR(2) DEFAULT 'IT',
        hire_date DATE
    );
    """

def test_parser_sql(simple_create_table):
    """Prueba que el parser SQL devuelve una lista de expresiones."""
    expressions = parser_sql(simple_create_table)
    assert isinstance(expressions, list)
    assert len(expressions) > 0
    assert all(isinstance(expr, Expression) for expr in expressions)