from engine.executor import PKAdmin
from query.parser_sql import parser_sql, get_values

create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
select_q = "SELECT name FROM ecm.store.employees WHERE name = 'Eva Liu';"
database_q = "CREATE DATABASE ecm"
schema_q = "CREATE SCHEMA ecm.store"
set_q = "SET search_path TO ecm.store"
index_q = "CREATE INDEX idx_name ON ecm.store.employees USING hash(name)"
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
    --db.sch
    CREATE SCHEMA ecm.store;
    --db.sch.table
    CREATE TABLE ecm.store.employees (
        id INT, --PK
        name VARCHAR(50), 
        salary DOUBLE
    );
    --CREATE INDEX idx_name ON ecm.store.employees USING hash(name);
    --CREATE INDEX idx_name ON ecm.store.employees USING btree(name);
    --CREATE INDEX idx_name ON ecm.store.employees USING isam(name);
    --CREATE INDEX idx_name ON ecm.store.employees USING rtree(name);

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
# se debe manejar los errores si es que ya existen
admin = PKAdmin()
admin.execute(select_q)
#table = admin.catalog.get_table("ecm", "store", "employees")
#print(table)

"""
logic plan: https://medium.com/@harun.raseed093/spark-logical-and-physical-plans-e111de6cc22e
"""