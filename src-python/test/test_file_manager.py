import os
import pytest
from storage.disk.file_manager import FileManager

@pytest.fixture
def tmp_dir(tmp_path):
    directory = tmp_path / "test_dir"
    directory.mkdir()
    return directory

@pytest.fixture
def file_manager(tmp_dir):
    """Fixture: FileManager instance."""
    return FileManager(base_path=tmp_dir)

def test_create_file(file_manager, tmp_dir):
    """Test: Create a file."""
    file_path = file_manager.base_path / "test_file.dat"
    file_manager.create_file(file_path)
    assert file_path.exists() 
    assert file_path.is_file()

def test_write_and_read_data(file_manager):
    """Test: Write and read data from a file."""
    file_path = file_manager.base_path / "data_file.dat"
    data = {"name": "Jose", "age": 21}

    file_manager.create_file(file_path)
    file_manager.write_data(data, file_path)
    read_data = file_manager.read_data(file_path)
    assert read_data == data

def test_delete_file(file_manager):
    """Test: Delete a file."""
    file_path = file_manager.base_path / "delete_me.dat"
    file_manager.create_file(file_path)
    assert file_path.exists()
    
    file_manager.delete_file(file_path)
    assert not file_path.exists()

def test_path_exists(file_manager):
    """Test: Check if a path exists using functions of file_manager."""
    file_path = file_manager.base_path / "check_file.dat"
    file_manager.create_file(file_path)

    assert file_manager.path_exists("check_file.dat")
    assert file_manager.path_exists(file_path)
