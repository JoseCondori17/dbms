from engine.executor import PKAdmin

create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
select_q = "SELECT * FROM ecm.store.employee WHERE id = 1"
database_q = "CREATE DATABASE ecm"
schema_q = "CREATE SCHEMA ecm.store"
set_q = "SET search_path TO ecm.store"

query = """
    -- SET search_path TO store;
    CREATE DATABASE ecm;
    CREATE SCHEMA ecm.store;
    CREATE TABLE ecm.store.employees (
        id INT, 
        name VARCHAR(50), 
        salary DECIMAL(10,2)
    );
"""
# se debe manejar los errores si es que ya existen
admin = PKAdmin()
admin.execute(query)