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
class Delete:
    catalog: CatalogManager

    def execute(self, expr: exp.Create) -> None:
        pass