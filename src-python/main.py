from engine.executor import PKAdmin
import time


create_q = "CREATE TABLE ecm.store.employees (id INT, name VARCHAR(50), salary DECIMAL(10,2))"
select_q = "SELECT name FROM ecm.store.employees WHERE name = 'Valerie Cook';"
select_q_btr = "SELECT name FROM ecm.store.employees WHERE id = 15;"
database_q = "CREATE DATABASE ecm"
schema_q = "CREATE SCHEMA ecm.store"
set_q = "SET search_path TO ecm.store"
index_q = "CREATE INDEX idx_name ON ecm.store.employees USING hash(name)"
insert_q = """
    INSERT INTO ecm.store.employees (id, name, salary) VALUES 
    (1, 'John Doe', 500.00), 
    (2, 'Jose Ede', 320.22),
    (3, 'Alice Smith', 450.75),
    ...
    (60, 'Hazel Peterson', 420.50);
"""
copy_q = "COPY ecm.store.employees FROM 'data/empleados.csv';"

query = """
    CREATE DATABASE ecm;
    CREATE SCHEMA ecm.store;
    CREATE TABLE ecm.store.employees (
        id INT,
        name VARCHAR(100), 
        salary DOUBLE
    );
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
    (10, 'Henry Adams', 430.60),
    (11, 'Isabel Cruz', 550.30),
    (12, 'Jack Wilson', 380.75),
    (13, 'Karen Lee', 490.50),
    (14, 'Luis Garcia', 420.00),
    (15, 'Megan Taylor', 530.25),
    (16, 'Nathan Brown', 360.90),
    (17, 'Olivia Davis', 470.40),
    (18, 'Paul Martinez', 440.15),
    (19, 'Quinn Anderson', 510.80),
    (20, 'Rachel White', 340.50),
    (21, 'Samuel Clark', 580.00),
    (22, 'Tina Rodriguez', 400.25),
    (23, 'Victor Hall', 460.70),
    (24, 'Wendy Young', 520.45),
    (25, 'Xavier Hernandez', 370.30),
    (26, 'Yvonne King', 540.60),
    (27, 'Zachary Wright', 390.85),
    (28, 'Anna Scott', 480.20),
    (29, 'Brian Green', 430.75),
    (30, 'Cynthia Baker', 560.00),
    (31, 'Daniel Gonzalez', 410.40),
    (32, 'Emily Nelson', 500.25),
    (33, 'Felix Carter', 350.90),
    (34, 'Gina Mitchell', 590.50),
    (35, 'Harry Perez', 440.30),
    (36, 'Irene Roberts', 470.80),
    (37, 'James Turner', 380.45),
    (38, 'Katherine Phillips', 520.10),
    (39, 'Leo Campbell', 450.60),
    (40, 'Mona Parker', 510.75),
    (41, 'Oscar Evans', 330.20),
    (42, 'Patricia Collins', 480.90),
    (43, 'Quentin Stewart', 570.40),
    (44, 'Rita Sanchez', 400.65),
    (45, 'Steve Morris', 540.25),
    (46, 'Tiffany Rogers', 460.50),
    (47, 'Ulysses Reed', 490.85),
    (48, 'Valerie Cook', 430.30),
    (49, 'Walter Morgan', 580.75),
    (50, 'Xena Bell', 410.90),
    (51, 'Yosef Murphy', 550.40),
    (52, 'Zoe Bailey', 370.60),
    (53, 'Aaron Rivera', 500.85),
    (54, 'Bianca Cooper', 440.20),
    (55, 'Cameron Richardson', 590.30),
    (56, 'Diana Cox', 390.45),
    (57, 'Ethan Howard', 530.70),
    (58, 'Fiona Ward', 460.25),
    (59, 'George Torres', 480.90),
    (60, 'Hazel Peterson', 420.50);
"""

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

#https://www.kaggle.com/datasets/salahuddinahmedshuvo/grocery-inventory-and-sales-dataset
inventory = """
CREATE TABLE products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    supplier_id VARCHAR(20),
    supplier_name VARCHAR(100),
    stock_quantity INT,
    reorder_level INT,
    reorder_quantity INT,
    unit_price DOUBLE,
    date_received DATE,
    last_order_date DATE,
    expiration_date DATE,
    warehouse_location VARCHAR(100),
    sales_volume INT,
    inventory_turnover_rate DOUBLE,
    status VARCHAR(20)
);
"""
create_index_hash = "CREATE INDEX idx_product_name ON ecm.store.products USING hash(product_name);"
select_query_hash_1 = "SELECT * FROM ecm.store.products WHERE product_name = 'Sushi Rice';"
select_query_hash_2 = "SELECT * FROM ecm.store.products WHERE product_name = 'Tilapia';"
select_query_hash_3 = "SELECT * FROM ecm.store.products WHERE product_name = 'Gouda Cheese';"
select_query_hash_4 = "SELECT * FROM ecm.store.products WHERE product_name = 'Carrot';"

## COMP
create_index_bptree = "CREATE INDEX idx_product_name ON ecm.store.products USING hash(product_name);"
select_query_bptree_1 = "SELECT * FROM ecm.store.products_bptree WHERE product_name = 'Sushi Rice';"
select_query_bptree_3 = "SELECT * FROM ecm.store.products WHERE product_name = 'Gouda Cheese';"
select_query_bptree_4 = "SELECT * FROM ecm.store.products_bptree WHERE product_name = 'Carrot';"

range_q = "SELECT * FROM ecm.store.employees WHERE id BETWEEN 5 AND 20;"
admin = PKAdmin()
start_time = time.time()
admin.execute(select_query_hash_3)
end_time = time.time()
print(f"Tiempo de ejecución de la consulta: {(end_time - start_time):.4f} segundos")

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
