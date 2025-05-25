# from engine.executor import PKAdmin
# from query.parser_sql import parser_sql, get_values

# create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
# select_q = "SELECT name FROM ecm.store.employees WHERE name = 'Eva Liu';"
# database_q = "CREATE DATABASE ecm"
# schema_q = "CREATE SCHEMA ecm.store"
# set_q = "SET search_path TO ecm.store"
# index_q = "CREATE INDEX idx_name ON ecm.store.employees USING hash(name)"
# insert_q = """
#     INSERT INTO ecm.store.employees (id, name, salary) VALUES 
#     (1, 'John Doe', 500.00), 
#     (2, 'Jose Ede', 320.22),
#     (3, 'Alice Smith', 450.75),
#     (4, 'Bob Johnson', 390.50),
#     (5, 'Carol Perez', 600.00),
#     (6, 'David Kim', 410.10),
#     (7, 'Eva Liu', 520.00),
#     (8, 'Frank Torres', 310.40),
#     (9, 'Grace Wong', 480.25),
#     (10, 'Henry Adams', 430.60);
# """

# query = """
#     -- SET search_path TO store;
#     CREATE DATABASE ecm;
#     --db.sch
#     CREATE SCHEMA ecm.store;
#     --db.sch.table
#     CREATE TABLE ecm.store.employees (
#         id INT, --PK
#         name VARCHAR(50), 
#         salary DOUBLE
#     );
#     --CREATE INDEX idx_name ON ecm.store.employees USING hash(name);
#     --CREATE INDEX idx_name ON ecm.store.employees USING btree(name);
#     --CREATE INDEX idx_name ON ecm.store.employees USING isam(name);
#     --CREATE INDEX idx_name ON ecm.store.employees USING rtree(name);

#     INSERT INTO ecm.store.employees (id, name, salary) VALUES 
#     (1, 'John Doe', 500.00), 
#     (2, 'Jose Ede', 320.22),
#     (3, 'Alice Smith', 450.75),
#     (4, 'Bob Johnson', 390.50),
#     (5, 'Carol Perez', 600.00),
#     (6, 'David Kim', 410.10),
#     (7, 'Eva Liu', 520.00),
#     (8, 'Frank Torres', 310.40),
#     (9, 'Grace Wong', 480.25),
#     (10, 'Henry Adams', 430.60);
# """
# # se debe manejar los errores si es que ya existen
# admin = PKAdmin()
# admin.execute(select_q)
# #table = admin.catalog.get_table("ecm", "store", "employees")
# #print(table)

# """
# logic plan: https://medium.com/@harun.raseed093/spark-logical-and-physical-plans-e111de6cc22e
# """

from engine.executor import PKAdmin
from query.parser_sql import parser_sql

query = """
--db
CREATE DATABASE ecm;

--db.sch
CREATE SCHEMA ecm.store;

--db.sch.table
CREATE TABLE ecm.store.employees (
    id INT,
    name VARCHAR(50),
    x DOUBLE,
    y DOUBLE,
    w DOUBLE,
    h DOUBLE
);

--db.sch.table.insert
INSERT INTO ecm.store.employees (id, name, x, y, w, h) VALUES 
(1, 'John Doe', 1, 1, 2, 2), 
(2, 'Jose Ede', 5, 5, 2, 2),
(3, 'Alice Smith', 10, 10, 2, 2),
(4, 'Bob Johnson', 7, 1, 3, 2),
(5, 'Carol Perez', 0, 0, 1, 1),
(6, 'David Kim', 9, 3, 1, 1),
(7, 'Eva Liu', 4, 4, 2, 2),
(8, 'Frank Torres', 3, 7, 2, 2),
(9, 'Grace Wong', 8, 8, 2, 2),
(10, 'Henry Adams', 2, 2, 2, 2);

--db.sch.table.index
CREATE INDEX idx_rtree ON ecm.store.employees USING RTREE (x, y, w, h);

--db.sch.table.query
SELECT name FROM ecm.store.employees 
WHERE x = 0 AND y = 0 AND w = 5 AND h = 5;
"""

# Ejecutar paso a paso
admin = PKAdmin()

for stmt in parser_sql(query):
    try:
        sql_text = stmt.sql(dialect="postgres")  # convierte a string SQL
        admin.execute(sql_text)
    except Exception as e:
        print(f"[ERROR] {type(stmt).__name__}: {e}")
