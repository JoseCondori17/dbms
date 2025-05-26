from engine.executor import PKAdmin
from query.parser_sql import parser_sql, get_values


# create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
# select_q = "SELECT name FROM ecm.store.employees WHERE name = 'Eva Liu';"
# select_q_btr = "SELECT name FROM ecm.store.employees WHERE id = 34;"
# database_q = "CREATE DATABASE ecm"
# schema_q = "CREATE SCHEMA ecm.store"
# set_q = "SET search_path TO ecm.store"
# index_q = "CREATE INDEX idx_name ON ecm.store.employees USING hash(name)"
# insert_q = """
#     INSERT INTO ecm.store.employees (id, name, salary) VALUES 
#     (1, 'John Doe', 500.00), 
#     (2, 'Jose Ede', 320.22),
#     (3, 'Alice Smith', 450.75),
#     ...
#     (60, 'Hazel Peterson', 420.50);
# """
# copy_q = "COPY ecm.store.employees FROM 'data/empleados.csv';"

# query = """
#     CREATE DATABASE ecm;
#     CREATE SCHEMA ecm.store;
#     CREATE TABLE ecm.store.employees (
#         id INT,
#         name VARCHAR(50), 
#         salary DOUBLE
#     );
#     INSERT INTO ecm.store.employees (id, name, salary) VALUES 
#     (1, 'John Doe', 500.00),
#     ...
#     (60, 'Hazel Peterson', 420.50);
# """


rtree_query = """
CREATE DATABASE geo;
CREATE SCHEMA geo.public;

CREATE TABLE geo.public.ciudades (
    id INT,
    name VARCHAR(50),
    latitude DOUBLE,
    longitude DOUBLE
);

INSERT INTO geo.public.ciudades (id, name, latitude, longitude) VALUES
(1, 'Lima', -12.0464, -77.0428),
(2, 'Cusco', -13.532, -71.967),
(3, 'Arequipa', -16.409, -71.537),
(4, 'Puno', -15.843, -70.021),
(5, 'Trujillo', -8.112, -79.028);

CREATE INDEX idx_ubicacion ON geo.public.ciudades USING rtree(latitude);

SELECT name FROM geo.public.ciudades 
WHERE latitude BETWEEN -16 AND -12 AND longitude BETWEEN -75 AND -70;
"""


admin = PKAdmin()

print("\n--- Ejecutando bloque: ciudades + RTree ---\n")
admin.execute(rtree_query)


# admin.execute(database_q)
# admin.execute(schema_q)
# admin.execute(create_q)
# admin.execute(index_q)
# admin.execute(insert_q)
# admin.execute(select_q)
# admin.execute(query)
# admin.execute(select_q_btr)

# table = admin.catalog.get_table("ecm", "store", "employees")
# print(table)

"""
logic plan: https://medium.com/@harun.raseed093/spark-logical-and-physical-plans-e111de6cc22e
"""
