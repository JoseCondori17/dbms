import os
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

from catalog.database import Database
from catalog.schema import Schema
from catalog.table import Table
from catalog.column import Column
from catalog.index import Index
from storage.disk.path_builder import PathBuilder
from storage.disk.file_manager import FileManager
from models.enum.data_type_enum import DataTypeTag
from models.enum.index_enum import IndexType
from storage.indexing.rtree_wrapper import RTree
from storage.indexing.bplus_tree import BPlusTreeFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.isam import ISAMFile

VERSION = "0.0.1"

@dataclass
class GlobalCatalog:
    databases: dict[str, Database]
    version: str
    created_at: datetime

class CatalogManager:
    def __init__(self, data_directory: Path):
        self.system_catalog_path = data_directory / "system/catalog.dat"
        self.file_manager        = FileManager(data_directory)
        self.path_builder        = PathBuilder(data_directory)
        
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
        path_db      = self.path_builder.database_dir(db_name)
        path_db_meta = self.path_builder.database_meta(db_name)
        self.file_manager.create_directory(path_db)
        self.file_manager.create_file(path_db_meta)
        db_id = self.generate_database_id()
        database = Database(db_id, db_name)
        self.global_catalog.databases[db_name] = database
        self.save_catalog()

    def create_schema(self, db_name: str, schema_name: str) -> None:
        path_sh      = self.path_builder.schema_dir(db_name, schema_name)
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        if not self.file_manager.path_exists(path_sh):
            self.file_manager.create_directory(path_sh)
        self.file_manager.create_file(path_sh_meta)
        schema_id = self.generate_schema_id(db_name)
        database  = self.global_catalog.databases[db_name]
        database.add_schema(schema_name, schema_id)
        path_db_meta = self.path_builder.database_meta(db_name)
        self.file_manager.write_data(database, path_db_meta)
        schema = Schema(schema_id, schema_name, database.get_id())
        self.file_manager.write_data(schema, path_sh_meta)
        self.save_catalog()

    def create_table(self,
                     db_name: str,
                     schema_name: str,
                     table_name: str,
                     columns:    list[Column],
                     indexes:    list[Index]
                     ) -> None:
        path_table      = self.path_builder.table_dir(db_name, schema_name, table_name)
        path_table_data = self.path_builder.table_data(db_name, schema_name, table_name)
        path_table_meta = self.path_builder.table_meta(db_name, schema_name, table_name)

        if not self.file_manager.path_exists(path_table):
            self.file_manager.create_directory(path_table)
        self.file_manager.create_file(path_table_data)
        self.file_manager.create_file(path_table_meta)

        table_id     = self.generate_table_id(db_name, schema_name)
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        schema_meta: Schema = self.file_manager.read_data(path_sh_meta)
        schema_meta.add_table(table_name, table_id)
        self.file_manager.write_data(schema_meta, path_sh_meta)
        table = Table(
            tab_id       = table_id,
            tab_name     = table_name,
            tab_namespace= schema_meta.get_id(),
            tab_tuples   = 0,
            tab_pages    = 1,
            tab_page_size= 8192,
            tab_columns  = columns,
            tab_indexes  = indexes
        )
        self.file_manager.write_data(table, path_table_meta)

    def create_index(self,
                     db_name:       str,
                     schema_name:   str,
                     table_name:    str,
                     index_name:    str,
                     index_type:    int,
                     index_colum:   int = 0,
                     index_is_primary: bool = False
                     ) -> None:
        path_meta   = self.path_builder.table_meta(db_name, schema_name, table_name)
        path_idx    = self.path_builder.table_index(db_name, schema_name, table_name, index_name)
        table: Table = self.file_manager.read_data(path_meta)
        
        idx_id = self.generate_index_id(db_name, schema_name, table_name)
        index = Index(
            idx_id       = idx_id,
            idx_type     = index_type,
            idx_name     = index_name,
            idx_file     = path_idx,
            idx_tuples   = 0,
            idx_columns  = [index_colum],
            idx_is_primary = index_is_primary
        )
        table.add_index(index)
        self.file_manager.write_data(table, path_meta)

        if index_type == IndexType.RTREE.value:
            RTree(filename=path_idx)
        else:
            self.file_manager.create_file(path_idx)

    # /////////////////////////////////////////////////////////////////////////////////////////    
    def get_version(self) -> str:
        return self.global_catalog.version

    def get_created_at(self) -> datetime:
        return self.global_catalog.created_at

    def get_database_names(self) -> list[str]:
        return list(self.global_catalog.databases.keys())

    def get_databases_json(self) -> str:
        result = []
        for db_name, db in self.global_catalog.databases.items():
            db_dict = asdict(db)
            if isinstance(db_dict.get("db_created_at"), datetime):
                db_dict["db_created_at"] = db_dict["db_created_at"].isoformat()
            result.append(db_dict)
        return result

    def get_schemas_dict(self, db_name: str) -> dict[str, int]:
        return self.global_catalog.databases[db_name].get_schemas()

    def get_schema(self, db_name: str, schema_name: str) -> Schema | None:
        path_sh_meta = self.path_builder.schema_meta(db_name, schema_name)
        return self.file_manager.read_data(path_sh_meta)

    def get_schemas(self, db_name: str) -> list[Schema]:
        schemas = []
        for name in self.get_schemas_dict(db_name):
            path_sh_meta = self.path_builder.schema_meta(db_name, name)
            schemas.append(self.file_manager.read_data(path_sh_meta))
        return schemas

    def get_schemas_json(self, db_name: str) -> str:
        return [asdict(s) for s in self.get_schemas(db_name)]

    def get_schemas_name(self, db_name: str) -> list[str]:
        return [s.get_name() for s in self.get_schemas(db_name)]

    def get_table(self, db_name: str, schema_name: str, table_name: str) -> Table:
        path_tab_meta = self.path_builder.table_meta(db_name, schema_name, table_name)
        return self.file_manager.read_data(path_tab_meta)

    def get_table_json(self, db_name: str, schema_name: str, table_name: str) -> Table:
        path_tab_meta = self.path_builder.table_meta(db_name, schema_name, table_name)
        table = self.file_manager.read_data(path_tab_meta)
        return asdict(table)

    def get_tables(self, db_name: str, schema_name: str) -> list[Table]:
        schema_meta: Schema = self.file_manager.read_data(
            self.path_builder.schema_meta(db_name, schema_name)
        )
        tables = []
        for name in schema_meta.get_tables():
            path_tab_meta = self.path_builder.table_meta(db_name, schema_name, name)
            tables.append(self.file_manager.read_data(path_tab_meta))
        return tables

    def get_tables_json(self, db_name: str, schema_name: str):
        return [asdict(t) for t in self.get_tables(db_name, schema_name)]

    def get_tables_name(self, db_name: str, schema_name: str) -> list[str]:
        return [t.get_tab_name() for t in self.get_tables(db_name, schema_name)]

    def get_position_column_by_name(self,
                                    db_name: str,
                                    schema_name: str,
                                    table_name: str,
                                    column_name: str
                                    ) -> int | None:
        table = self.get_table(db_name, schema_name, table_name)
        for i, col in enumerate(table.get_tab_columns()):
            if col.get_att_name() == column_name:
                return i
        return None

    def callbacks_index(self,
                        db_name:     str,
                        schema_name: str,
                        table_name:  str
                        ) -> dict:
        table   = self.get_table(db_name, schema_name, table_name)
        columns = table.get_tab_columns()
        callbacks = {}

        for index in table.get_tab_indexes():
            col_pos  = index.get_idx_columns()[0]
            column   = columns[col_pos]
            dtype    = column.get_att_to_type_id()
            max_len  = column.get_att_len()
            idx_type = index.get_idx_type()
            idx_file = str(index.get_idx_file())

            if idx_type in (IndexType.SEQUENTIAL.value, IndexType.HASH.value):
                idx_obj = ExtendibleHashingFile(
                    index_filename=idx_file,
                    data_type=dtype,
                    max_key_len=max_len
                )
            elif idx_type == IndexType.ISAM.value:
                idx_obj = ISAMFile(
                    index_filename=idx_file,
                    data_type=dtype,
                    max_key_len=max_len
                )
            elif idx_type == IndexType.BTREE.value:
                idx_obj = BPlusTreeFile(
                    index_filename=idx_file,
                    data_type=dtype,
                    max_key_len=max_len,
                    order=4
                )
            elif idx_type == IndexType.RTREE.value:
                idx_obj = RTree(filename=idx_file)
            else:
                idx_obj = ExtendibleHashingFile(
                    index_filename=idx_file,
                    data_type=dtype,
                    max_key_len=max_len
                )

            callbacks[index.get_idx_id()] = (idx_obj, col_pos)

        return callbacks

    # /////////////////////////////////////////////////////////////////////////////////////////
    def generate_database_id(self) -> int:
        return max((db.get_id() for db in self.global_catalog.databases.values()), default=0) + 1
    def generate_schema_id(self, db_name: str) -> int:
        return max(self.global_catalog.databases[db_name].get_schemas().values(), default=0) + 1
    def generate_table_id(self, db_name: str, schema_name: str) -> int:
        schema_meta: Schema = self.file_manager.read_data(
            self.path_builder.schema_meta(db_name, schema_name)
        )
        return max(schema_meta.get_tables().values(), default=0) + 1
    def generate_index_id(self, db_name: str, schema_name: str, table_name) -> int:
        table_meta: Table = self.file_manager.read_data(
            self.path_builder.table_meta(db_name, schema_name, table_name)
        )
        return max((i.get_idx_id() for i in table_meta.get_tab_indexes()), default=0) + 1