from engine.executor import PKAdmin

def main():
    admin = PKAdmin()

    statements = [
        # 1) Crear base de datos y esquema
        "CREATE DATABASE testdb;",
        "CREATE SCHEMA testdb.public;",

        # 2) Crear tabla
        "CREATE TABLE testdb.public.users (id INT, name VARCHAR(20));",

        # 3) Insertar valores
        "INSERT INTO testdb.public.users (id, name) VALUES "
        "(1, 'Ana'), "
        "(2, 'Bob'), "
        "(3, 'Cara'), "
        "(4, 'Diana'), "
        "(5, 'Evan');",

        # 4) Crear índice AVL sobre la columna id
        "CREATE INDEX idx_id_avl ON testdb.public.users USING avl(id);",

        # 5) SELECT por clave (usando índice AVL)
        "SELECT name FROM testdb.public.users WHERE id = 3;",

        # 6) SELECT por rango (usando índice AVL)
        "SELECT name FROM testdb.public.users WHERE id BETWEEN 2 AND 4;",

        # 7) DELETE con índice AVL
        "DELETE FROM testdb.public.users WHERE id = 2;",

        # 8) SELECT para verificar eliminación
        "SELECT name FROM testdb.public.users WHERE id BETWEEN 1 AND 5;"
    ]

    for sql in statements:
        print(f"--- Ejecutando:\n{sql}")
        try:
            admin.execute(sql)
        except Exception as e:
            print(f"❌ Error al ejecutar: {e}")
        print()

if __name__ == "__main__":
    main()
