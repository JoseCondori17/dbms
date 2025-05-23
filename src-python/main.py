from engine.executor import PKAdmin
from query.parser_sql import parser_sql, get_values

create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
select_q = "SELECT * FROM ecm.store.employee WHERE id = 1"
database_q = "CREATE DATABASE ecm"
schema_q = "CREATE SCHEMA ecm.store"
set_q = "SET search_path TO ecm.store"
insert_q = """
    INSERT INTO ecm.store.employees (id, name, salary) VALUES 
    (1, 'John Doe', 500.00), 
    (2, 'Jose Ede', 320.22),
    (3, 'Alice Smith', 450.75),
    (4, 'Bob Johnson', 390.50),
    (5, 'Carol Perez', 600.00),
    (6, 'David Kim', 410.10),
    (7, 'Eva Liu', 520.00),
    (8, 'Frank Torres', 310.40),
    (9, 'Grace Wong', 480.25),
    (10, 'Henry Adams', 430.60);
"""

query = """
    -- SET search_path TO store;
    CREATE DATABASE ecm;
    CREATE SCHEMA ecm.store;
    CREATE TABLE ecm.store.employees (
        id INT, 
        name VARCHAR(50), 
        salary DOUBLE
    );
"""
# se debe manejar los errores si es que ya existen
admin = PKAdmin()
admin.execute(insert_q)
