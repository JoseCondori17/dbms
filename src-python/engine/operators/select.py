from dataclasses import dataclass
from sqlglot import expressions as exp

from catalog.table import Table
from catalog.catalog_manager import CatalogManager
from models.enum.index_enum import IndexType
from storage.indexing.heap import HeapFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.bplus_tree import BPlusTreeFile
from storage.indexing.isam import ISAMFile
from engine.planner import login_plan
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_identifier
)

@dataclass
class Select:
    catalog: CatalogManager
    
    def execute(self, expr: exp.Select):
        db_name = get_table_catalog(expr)
        schema_name = get_table_schema(expr)
        table_name = get_table_name(expr)
        path_data = self.catalog.path_builder.table_data(db_name, schema_name, table_name)
        table: Table = self.catalog.get_table(db_name, schema_name, table_name)
        columns = table.get_tab_columns()

        plan = login_plan(expr)
        column_name: str = get_identifier(plan.condition)
        index = self.get_index(column_name, table)
        index_type = index.get_idx_type()
        if index_type == IndexType.BTREE.value:
            key: str = plan.condition.expression.to_py()
            record = self.call_btree(table, index, path_data, key)
            print(record)
            # between
            # inorder [1,2,3,4,5...]
        elif index_type == IndexType.HASH.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            record = self.call_hash(table, index.get_idx_file(), path_data ,column.get_att_len(), key)
            print(record)
        elif index_type == IndexType.ISAM.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            records = self.call_isam()
            print(records)
        elif index_type == IndexType.RTREE.value:
            pass

    def get_index(self, column_name: str, table: Table):
        pos = 0
        for i, column in enumerate(table.get_tab_columns()):
            if column.get_att_name() == column_name:
               pos = i
               break
        indexes = table.get_tab_indexes()
        if (pos + 1) > len(indexes):
            return indexes[0]
        return indexes[pos]

    def call_hash(self, table, index_file, data_file, max_key: int, key: str) -> None:
        hash_file = ExtendibleHashingFile(index_file, max_key)
        heap_file = HeapFile(table, data_file)
        pos = hash_file.search(key)
        if pos is None:
            return None
        record = heap_file.read_record(pos)
        return record
    
    def call_btree(self, table: Table, index_obj, data_file: str, key: str):
        col_pos    = index_obj.get_idx_columns()[0]
        column     = table.get_tab_columns()[col_pos]
        idx_path   = index_obj.get_idx_file()
        btree = BPlusTreeFile(
            index_filename=str(idx_path),
            max_key_size=column.get_att_len(),
            order=4
        )
        heap = HeapFile(table, data_file)
        pos  = btree.search(key)
        return None if pos is None else heap.read_record(pos)

    def call_isam(): pass
