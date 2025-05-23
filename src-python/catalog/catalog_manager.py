import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional
from dataclasses import dataclass

from catalog.database import Database
from catalog.schema import Schema
from catalog.table import Table
from catalog.column import Column
from catalog.index import Index
from storage.disk.path_builder import PathBuilder
from storage.disk.file_manager import FileManager

VERSION = "0.0.1"

@dataclass
class GlobalCatalog:
    databases: Dict[str, Database]
    version: str
    created_at: datetime

class CatalogManager:
    def __init__(self, data_directory: Path):
        self.system_catalog_path = data_directory / "system/catalog.dat"
        self.file_manager = FileManager(data_directory)
        self.path_builder = PathBuilder(data_directory)
        
        if not data_directory.exists():
            os.makedirs(data_directory, exist_ok=True)

        if not self.file_manager.path_exists(Path("system")):
            path_system = self.path_builder.system_dir("system")
            self.file_manager.create_directory(path_system)

        self.global_catalog = GlobalCatalog(
            databases={},
            version=VERSION,
            created_at=datetime.now(timezone.utc)
        )
        if self.file_manager.path_exists(Path("system/catalog.dat")):
            self.load_catalog()
        else:
            self.save_catalog()

    # /////////////////////////////////////////////////////////////////////////////////////////
    def load_catalog(self) -> None:
        self.global_catalog = self.file_manager.read_data(self.system_catalog_path)
        
    def save_catalog(self) -> None:
        catalog_path = Path("system/catalog.dat")
        if not self.file_manager.path_exists(catalog_path):
            self.file_manager.create_file(self.system_catalog_path)
        self.file_manager.write_data(self.global_catalog, self.system_catalog_path)

    # /////////////////////////////////////////////////////////////////////////////////////////
    def create_database(self, db_name: str) -> None:
        path_db = self.path_builder.database_dir(db_name)
        path_db_meta = self.path_builder.database_meta(db_name)
        self.file_manager.create_directory(path_db)
        self.file_manager.create_file(path_db_meta)
        db_id = self.generate_database_id()
        database = Database(db_id, db_name)
        self.global_catalog.databases[db_name] = database
        self.save_catalog()

    def create_schema(self, db_name: str, schema_name: str) -> None:
        path_sh = self.path_builder.schema_dir(db_name, schema_name)
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        if not self.file_manager.path_exists(path_sh):
            self.file_manager.create_directory(path_sh)
        self.file_manager.create_file(path_sh_meta)
        schema_id = self.generate_schema_id(db_name)
        database = self.global_catalog.databases[db_name]
        database.add_schema(schema_name, schema_id)
        # Update database metadata
        path_db_meta = self.path_builder.database_meta(db_name)
        self.file_manager.write_data(database, path_db_meta)
        # Write schema metadata
        schema = Schema(schema_id, schema_name, database.get_id())
        self.file_manager.write_data(schema, path_sh_meta)
        # Save database catalog data
        self.save_catalog()

    def create_table(self, db_name: str, schema_name: str, table_name: str, columns: list[Column], indexes: list[Index]) -> None:
        path_table = self.path_builder.table_dir(db_name, schema_name, table_name)
        path_table_data = self.path_builder.table_data(db_name, schema_name, table_name)
        path_table_meta = self.path_builder.table_meta(db_name, schema_name, table_name)
        #path_table_idx_pk = self.path_builder.table_index(db_name, schema_name, table_name, "pk")
        if not self.file_manager.path_exists(path_table):
            self.file_manager.create_directory(path_table)
        self.file_manager.create_file(path_table_data)
        self.file_manager.create_file(path_table_meta)
        #self.file_manager.create_file(path_table_idx_pk)
        # Update schema metadata
        table_id = self.generate_table_id(db_name, schema_name)
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        schema_meta: Schema = self.file_manager.read_data(path_sh_meta)
        schema_meta.add_table(table_name, table_id)
        self.file_manager.write_data(schema_meta, path_sh_meta)
        # Write table header metadata
        table = Table(
            tab_id=table_id,
            tab_name=table_name,
            tab_namespace=schema_meta.get_id(),
            tab_tuples=0,
            tab_pages=1, 
            tab_page_size=8192,
            tab_columns=columns,
            tab_indexes=indexes
        )
        self.file_manager.write_data(table, path_table_meta)

    # pending
    def create_index(self, db_name: str, schema_name: str, table_name: str, index_name: str) -> None:
        path_idx_name = self.path_builder.table_index(db_name, schema_name, table_name, index_name)
        self.file_manager.create_file(path_idx_name)

    def create_function(self, db_name: str, schema_name: str, table_name: str, function_name: str) -> None:
        path_fn_name = self.path_builder.table_index(db_name, schema_name, table_name, function_name)
        self.file_manager.create_file(path_fn_name)

    # /////////////////////////////////////////////////////////////////////////////////////////
    def get_version(self) -> str:
        return self.global_catalog.version
    
    def get_created_at(self) -> datetime:
        return self.global_catalog.created_at

    def get_database_names(self) -> list[str]:
        return list(self.global_catalog.databases.keys())
    
    def get_schemas_dict(self, db_name: str) -> Dict[str, int]:
        database = self.global_catalog.databases[db_name]
        return database.get_schemas()
    
    def get_schema(self, db_name: str, schema_name: str) -> Optional[Schema]:
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        return self.file_manager.read_data(path_sh_meta)
    
    def get_table(self, db_name: str, schema_name: str, table_name: str) -> Optional[Table]:
        path_tab_meta = self.path_builder.table_meta(db_name, schema_name, table_name)
        table: Table = self.file_manager.read_data(path_tab_meta)
        return table

    def get_tables(self, db_name: str, schema_name: str) -> list[Table]:
        database = self.global_catalog.databases[db_name]
        schema = self.path_builder.schema_meta(db_name, schema_name)
        schema_meta: Schema = self.file_manager.read_data(schema)
        tables = []
        for table_name in schema_meta.get_tables().keys():
            path_tab_meta = self.path_builder.table_meta(db_name, schema_name, table_name)
            table: Table = self.file_manager.read_data(path_tab_meta)
            tables.append(table)
        return tables
    
    # /////////////////////////////////////////////////////////////////////////////////////////
    def generate_database_id(self) -> int:
        return max((db.get_id() for db in self.global_catalog.databases.values()), default=0) + 1
    def generate_schema_id(self, db_name: str) -> int:
        database = self.global_catalog.databases[db_name]
        return max((schema_id for schema_id in database.get_schemas().values()), default=0) + 1
    def generate_table_id(self, db_name: str, schema_name: str) -> int:
        path_schema = self.path_builder.schema_meta(db_name, schema_name)
        schema_meta: Schema = self.file_manager.read_data(path_schema)
        return max((table_id for table_id in schema_meta.get_tables().values()), default=0) + 1
    def generate_function_id(self, db_name: str, schema_name: str) -> int:
        """ path_schema = self.path_builder.function_file(db_name, schema_name)
        schema_meta: Schema = self.file_manager.read_data(path_schema)
        return max((function_id for function_id in schema_meta.get_functions().values()), default=0) + 1 """
        pass
    def generate_index_id(self, db_name: str, schema_name: str, table_name) -> int:
        path_schema = self.path_builder.table_meta(db_name, schema_name, table_name)
        table_meta: Table = self.file_manager.read_data(path_schema)
        return max((index.get_idx_id() for index in table_meta.get_tab_indexes()), default=0) + 1