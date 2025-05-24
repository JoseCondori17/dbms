from dataclasses import dataclass
from sqlglot import expressions as exp

from catalog.table import Table
from catalog.catalog_manager import CatalogManager
from models.enum.index_enum import IndexType
from storage.indexing.heap import HeapFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.btree import BTreeFile
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
            pass
        elif index_type == IndexType.HASH.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            record = self.call_hash(table, index.get_idx_file(), path_data ,column.get_att_len(), key)
            print(record)
        elif index_type == IndexType.ISAM.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            records = self.call_isam()

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
    
    def call_btree(self, table, index_file, data_file, key: str):
        btree_file = BTreeFile(index_filename=index_file)
        heap_file = HeapFile(table, data_file)
        results = btree_file.search(key)
        output = []
        for pos in results:
            record = heap_file.read_record(pos)
            if record is not None:
                output.append(record)
        return output

    def call_isam(): pass
