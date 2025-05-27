

from engine.executor import PKAdmin

admin = PKAdmin()

# Crear base de datos y esquema
admin.execute("CREATE DATABASE ecm;")
admin.execute("CREATE SCHEMA ecm.store;")

# Crear tabla
admin.execute("""
    CREATE TABLE ecm.store.employees (
        id INT,
        name VARCHAR(50), 
        salary DOUBLE
    );
""")

# Insertar registros
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
    (40, 'Mona Parker', 510.75);
"""
admin.execute(insert_q)

# Crear índice AVL sobre la columna 'id'
admin.execute("CREATE INDEX idx_id_avl_fix_xd ON ecm.store.employees USING avl(id);")
admin.execute("DELETE FROM ecm.store.employees WHERE id = 15;")
admin.execute("SELECT name FROM ecm.store.employees WHERE id = 15;")


# SELECT por igualdad (debe devolver un registro)
#admin.execute("SELECT name FROM ecm.store.employees WHERE id = 34;")

# SELECT con BETWEEN (debe devolver varios)
#admin.execute("SELECT name FROM ecm.store.employees WHERE id BETWEEN 30 AND 35;")
