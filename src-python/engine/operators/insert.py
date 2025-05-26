from dataclasses import dataclass
from sqlglot import expressions as exp

from storage.indexing.hashing import ExtendibleHashingFile
from catalog.catalog_manager import CatalogManager
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_values,
    to_tuple,
)
from storage.indexing.heap import HeapFile

@dataclass
class Insert:
    catalog: CatalogManager

    def execute_from_tuple(self, db_name, schema_name, table_name, values: tuple[any, ...]) -> None:
        table = self.catalog.get_table(db_name, schema_name, table_name)
        table_path = self.catalog.path_builder.table_data(db_name, schema_name, table_name)
        indexes = self.catalog.callbacks_index(db_name, schema_name, table_name)

        with HeapFile(table, table_path) as heap:
            position = heap.insert(values)
            for index, pos_column in indexes.values():
                index.insert(values[pos_column], position)

    def execute(self, expr: exp.Insert) -> None:
        db_name = get_table_catalog(expr)
        schema_name = get_table_schema(expr)
        table_name = get_table_name(expr)

        table = self.catalog.get_table(
            db_name,
            schema_name,
            table_name,
        )

        table_path = self.catalog.path_builder.table_data(
            db_name,
            schema_name,
            table_name,
        )
        indexes = self.catalog.callbacks_index(
            db_name,
            schema_name,
            table_name
        )
        with HeapFile(table, table_path) as heap:
            values = get_values(expr)
            for value in values:
                value = to_tuple(value)
                position = heap.insert(value)
                # update all indexes
                for index, pos_column in indexes.values():
                    index.insert(value[pos_column], position)