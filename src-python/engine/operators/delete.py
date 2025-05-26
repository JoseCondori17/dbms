from dataclasses import dataclass
from sqlglot import expressions as exp

from catalog.catalog_manager import CatalogManager
from storage.indexing.heap import HeapFile
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_identifier,
)

@dataclass
class Delete:
    catalog: CatalogManager

    def execute(self, expr: exp.Delete) -> str:
        db_name     = get_table_catalog(expr)
        schema_name = get_table_schema(expr)
        table_name  = get_table_name(expr)

        where = expr.args.get("where")
        if not where or not where.this:
            raise ValueError("DELETE requiere una cláusula WHERE")
        cond     = where.this
        col_name = get_identifier(cond)
        value    = cond.expression.to_py()

        table     = self.catalog.get_table(db_name, schema_name, table_name)
        data_path = self.catalog.path_builder.table_data(db_name, schema_name, table_name)
        callbacks = self.catalog.callbacks_index(db_name, schema_name, table_name)

        deleted = 0
        with HeapFile(table, data_path) as heap:
            rid = 0
            while True:
                rec = heap.read_record(rid)
                if rec is None:
                    break
                row, active = rec
                col_pos = self.catalog.get_position_column_by_name(db_name, schema_name, table_name, col_name)
                if active and row[col_pos] == value:
                    heap.delete(rid)
                    deleted += 1
                    for idx_obj, pos in callbacks.values():
                        idx_obj.delete(str(row[pos]))
                rid += 1

        return f"{deleted} rows deleted"
