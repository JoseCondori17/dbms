from engine.executor import PKAdmin

def main():
    admin = PKAdmin()

    statements = [
        # 1) Create a fresh database & schema
        "CREATE DATABASE testdb;",
        "CREATE SCHEMA testdb.public;",

        # 2) Create a simple table
        "CREATE TABLE testdb.public.u (id INT, name VARCHAR(20));",

        # 3) Bulk‐insert a few rows
        "INSERT INTO testdb.public.u (id, name) VALUES "
        "(1, 'Ana'), "
        "(2, 'Bob'), "
        "(3, 'Cara');",

        # 4) Build a B+Tree index on the name column
        "CREATE INDEX ix_name ON testdb.public.u USING btree(name);",

        # 5) Query via the index
        "SELECT * FROM testdb.public.u WHERE name = 'Ana';",
    ]

    for sql in statements:
        print(f"--- Ejecutando:\n{sql}\n")
        admin.execute(sql)
        print()

if __name__ == "__main__":
    main()
