# test_delete_operator.py
import shutil
import pytest
from catalog.catalog_manager import CatalogManager
from engine.executor import PKAdmin

@pytest.fixture
def admin(tmp_path):
    cm    = CatalogManager(tmp_path)
    admin = PKAdmin(catalog=cm)
    admin.execute("CREATE DATABASE testdb;")
    admin.execute("CREATE SCHEMA testdb.public;")
    yield admin
    shutil.rmtree(tmp_path, ignore_errors=True)

def test_delete_without_index(admin):
    admin.execute("CREATE TABLE testdb.public.users (id INT, name VARCHAR(10));")
    admin.execute("""
        INSERT INTO testdb.public.users (id, name) VALUES
        (1, 'Alice'),
        (2, 'Bob');
    """)
    # Primera llamada borra a Alice
    res1 = admin.execute("DELETE FROM testdb.public.users WHERE name = 'Alice';")
    assert res1 == "1 rows deleted"
    # Segunda llamada ya no encuentra a Alice
    res2 = admin.execute("DELETE FROM testdb.public.users WHERE name = 'Alice';")
    assert res2 == "0 rows deleted"

def test_delete_with_hash_index(admin):
    admin.execute("CREATE TABLE testdb.public.items (sku INT, descr VARCHAR(20));")
    admin.execute("""
        INSERT INTO testdb.public.items (sku, descr) VALUES
        (100, 'Widget'),
        (200, 'Gadget');
    """)
    admin.execute("CREATE INDEX idx_descr ON testdb.public.items USING hash(descr);")
    # Borro con índice hash
    res1 = admin.execute("DELETE FROM testdb.public.items WHERE descr = 'Gadget';")
    assert res1 == "1 rows deleted"
    # Borro la otra fila por pk
    res2 = admin.execute("DELETE FROM testdb.public.items WHERE sku = 100;")
    assert res2 == "1 rows deleted"
    # Ya nada queda
    res3 = admin.execute("DELETE FROM testdb.public.items WHERE descr = 'Gadget';")
    assert res3 == "0 rows deleted"
