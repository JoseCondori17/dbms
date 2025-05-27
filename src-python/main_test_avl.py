from engine.executor import PKAdmin

admin = PKAdmin()

# 1. Crear la base de datos y esquema
admin.execute("CREATE DATABASE testdb;")
admin.execute("CREATE SCHEMA testdb.public;")

# 2. Crear la tabla
admin.execute("CREATE TABLE testdb.public.users (id INT, name VARCHAR(20));")

# 3. Insertar algunos datos
admin.execute("""
    INSERT INTO testdb.public.users (id, name) VALUES 
    (1, 'Ana'), 
    (2, 'Bob'), 
    (3, 'Cara'), 
    (4, 'Diana'), 
    (5, 'Evan');
""")

# 4. Crear índice AVL sobre id
admin.execute("CREATE INDEX idx_id_avl ON testdb.public.users USING avl(id);")

# 5. SELECT por igualdad (debe devolver a Cara)
admin.execute("SELECT name FROM testdb.public.users WHERE id = 3;")

# 6. SELECT por rango (debe devolver Bob, Cara, Diana)
admin.execute("SELECT name FROM testdb.public.users WHERE id BETWEEN 2 AND 4;")

# 7. DELETE a Bob
admin.execute("DELETE FROM testdb.public.users WHERE id = 2;")

# 8. SELECT por rango nuevamente (debe devolver solo Cara y Diana)
admin.execute("SELECT name FROM testdb.public.users WHERE id BETWEEN 2 AND 4;")
