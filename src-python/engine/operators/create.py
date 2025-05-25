from dataclasses import dataclass 
from sqlglot import expressions as exp
import os

from catalog.catalog_manager import CatalogManager
from catalog.column import Column
from storage.indexing.heap import HeapFile
from storage.indexing.isam import ISAMFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.bplus_tree import BPlusTreeFile
from models.enum.index_enum import IndexType
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_columns,
    get_name,
    get_index_type,
    get_column_name,
)

@dataclass
class Create:
    catalog: CatalogManager

    def execute(self, expr: exp.Create) -> None:
        fn = expr.args.get("kind")
        if fn == "TABLE":
            self._create_table(expr)
            return "table created"
        elif fn == "INDEX":
            self._create_index(expr)
            return "index created"
        elif fn == "SCHEMA":
            self.catalog.create_schema(
                db_name=get_table_catalog(expr), 
                schema_name=get_table_schema(expr)
            )
            return "schema created"
        elif fn == "DATABASE":
            self.catalog.create_database(db_name=get_name(expr))
            return "database created"
        else:
            raise ValueError(f"Unknown create kind: {fn}")
        
    def _create_table(self, expr: exp.Create):
        db_name: str = get_table_catalog(expr)
        schema_name: str = get_table_schema(expr)
        table_name: str = get_table_name(expr)
        index_default_name = "pk"

        self.catalog.create_table(
            db_name,
            schema_name,
            table_name,
            columns=get_columns(expr),
            indexes=[],
        )
        self.catalog.create_index(
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            index_name=index_default_name,
            index_type=IndexType.BTREE.value,
            index_colum=0,
            index_is_primary=True,
        )

    def _create_index(self, expr: exp.Create):
        db_name: str = get_table_catalog(expr)
        schema_name: str = get_table_schema(expr)
        table_name: str = get_table_name(expr)
        index_name: str = get_name(expr)
        index_type: str = get_index_type(expr).upper()
        column_name: str = get_column_name(expr).lower()
        index_column = self.catalog.get_position_column_by_name(
            db_name,
            schema_name,
            table_name,
            column_name
        )
        self.catalog.create_index(
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            index_name=index_name,
            index_type=IndexType[index_type].value,
            index_colum=index_column,
            index_is_primary=False,
        )
        path_data = self.catalog.path_builder.table_data(db_name, schema_name, table_name)
        path_index = self.catalog.path_builder.table_index(db_name, schema_name, table_name, index_name)

        if os.path.getsize(path_data) != 0:
            table = self.catalog.get_table(db_name, schema_name, table_name)
            with HeapFile(table, path_data) as heap:
                if IndexType[index_type] == IndexType.ISAM:
                    block_factor = 10                    
                    column: Column = table.get_tab_columns()[index_column]
                    isam_file = ISAMFile(index_filename=path_index, max_key_size=column.get_att_len())

                    record_id = 0
                    records_processed = 0
                    
                    while True:
                        record_data = heap.read_record(record_id)
                        if record_data is None:
                            break
                        data_tuple, is_active = record_data
                        if is_active:
                            key = data_tuple[index_column]
                            if records_processed % block_factor == 0:
                                logical_position = records_processed // block_factor
                                isam_file.insert(key, logical_position)
                            records_processed += 1
                        record_id += 1

                elif IndexType[index_type] == IndexType.BTREE:
                    column: Column = table.get_tab_columns()[index_column]
                    btree_file = BPlusTreeFile( index_filename=path_index,max_key_size=column.get_att_len(), order=4)                    
                    record_id = 0
                    while True:
                        record_data = heap.read_record(record_id)
                        if record_data is None:
                            break
                        data_tuple, is_active = record_data
                        if is_active:
                            key = data_tuple[index_column]
                            btree_file.insert(key, record_id)
                        record_id += 1

                elif IndexType[index_type] == IndexType.HASH:
                    column: Column = table.get_tab_columns()[index_column]
                    hash_file = ExtendibleHashingFile(index_filename=path_index, max_key_size=column.get_att_len())

                    record_id = 0
                    while True:
                        record_data = heap.read_record(record_id)
                        if record_data is None:
                            break
                        data_tuple, is_active = record_data
                        if is_active:
                            key = data_tuple[index_column]
                            hash_file.insert(key, record_id)
                        record_id += 1

                elif IndexType[index_type] == IndexType.RTREE:
                    pass
            print(index_type)
