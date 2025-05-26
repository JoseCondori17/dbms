from engine.executor import PKAdmin

admin = PKAdmin()

# --- setup
admin.execute("CREATE DATABASE testdb;")
admin.execute("CREATE SCHEMA testdb.public;")
admin.execute("CREATE TABLE testdb.public.u (id INT, name VARCHAR(20));")
admin.execute("INSERT INTO testdb.public.u (id, name) VALUES (1, 'Ana'), (2, 'Bob');")
admin.execute("CREATE INDEX ix_name ON testdb.public.u USING btree(name);")

# --- pruebas
print("BEFORE DELETE:", admin.execute("SELECT * FROM testdb.public.u WHERE name = 'Ana';"))
print("DELETE:",        admin.execute("DELETE FROM testdb.public.u WHERE name = 'Ana';"))
print("AFTER DELETE:",  admin.execute("SELECT * FROM testdb.public.u WHERE name = 'Ana';"))

# --- prueba adicional: borrar por ID
print("DELETE BY ID:",       admin.execute("DELETE FROM testdb.public.u WHERE id = 2;"))
print("AFTER DELETE BY ID:", admin.execute("SELECT * FROM testdb.public.u WHERE id = 2;"))
