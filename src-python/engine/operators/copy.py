from dataclasses import dataclass
import csv
from pathlib import Path
from sqlglot import expressions as exp
from catalog.catalog_manager import CatalogManager
from engine.operators.insert import Insert
from query.parser_sql import get_copy_info

@dataclass
class Copy:
    catalog: CatalogManager

    def execute(self, expr: exp.Copy):
        db, schema, table, filepath = get_copy_info(expr)
        insert_engine = Insert(self.catalog)

        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                insert_engine.execute_from_tuple(
                    db_name=db,
                    schema_name=schema,
                    table_name=table,
                    values=tuple(row)
                )