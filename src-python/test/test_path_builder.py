import pytest
from pathlib import Path
from storage.disk.path_builder import PathBuilder

@pytest.fixture
def tmp_dir(tmp_path):
    """Fixture: Create a temporary directory for testing."""
    return tmp_path / "test_dir"

@pytest.fixture
def path_builder(tmp_dir):
    """Fixture: PathBuilder instance"""
    return PathBuilder(base_dir=tmp_dir)

def test_get_base_dir(path_builder, tmp_dir):
    """Test: Verify the base directory is set correctly."""
    assert path_builder.get_base_dir() == tmp_dir

def test_system_dir(path_builder):
    """Test: Verify the system directory is constructed correctly."""
    system_name = "test_system"
    expected = path_builder.base_dir / system_name
    assert path_builder.system_dir(system_name) == expected

def test_database_dir(path_builder):
    """Test: Verify the database directory is constructed correctly."""
    db_name = "test_db"
    expected = path_builder.base_dir / f"db_{db_name}"
    assert path_builder.database_dir(db_name) == expected

def test_database_meta(path_builder):
    """Test: Verify the database meta file path is constructed correctly."""
    db_name = "test_db"
    expected = path_builder.base_dir / f"db_{db_name}" / "meta.dat"
    assert path_builder.database_meta(db_name) == expected

def test_schema_dir(path_builder):
    """Test: Verify the schema directory is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}"
    assert path_builder.schema_dir(db_name, schema_name) == expected

def test_schema_meta(path_builder):
    """Test: Verify the schema meta file path is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    excepted = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / "meta.dat"
    assert path_builder.schema_meta(db_name, schema_name) == excepted

def test_table_dir(path_builder):
    """Test: Verify the table directory is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    table_name = "test_table"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / f"table_{table_name}"
    assert path_builder.table_dir(db_name, schema_name, table_name) == expected

def test_table_data(path_builder):
    """Test: Verify the table data file path is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    table_name = "test_table"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / f"table_{table_name}" / "data.dat"
    assert path_builder.table_data(db_name, schema_name, table_name) == expected

def test_table_meta(path_builder):
    """Test: Verify the table meta file path is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    table_name = "test_table"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / f"table_{table_name}" / "meta.dat"
    assert path_builder.table_meta(db_name, schema_name, table_name) == expected

def test_table_index(path_builder):
    """Test: Verify the table index file path is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    table_name = "test_table"
    property = "test_property"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / f"table_{table_name}" / f"idx_{property}_{table_name}.dat"
    assert path_builder.table_index(db_name, schema_name, table_name, property) == expected

def test_function_file(path_builder):
    """Test: Verify the function file path is constructed correctly."""
    db_name = "test_db"
    schema_name = "test_schema"
    table_name = "test_table"
    function_name = "test_function"
    expected = path_builder.base_dir / f"db_{db_name}" / f"schema_{schema_name}" / f"fn_{function_name}_{table_name}.dat"
    assert path_builder.function_file(db_name, schema_name, table_name, function_name) == expected