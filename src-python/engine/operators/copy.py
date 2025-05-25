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
        table_obj = self.catalog.get_table(db, schema, table)
        insert_engine = Insert(self.catalog)
        table_columns = table_obj.get_tab_columns()

        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                insert_engine.execute_from_tuple(
                    db_name=db,
                    schema_name=schema,
                    table_name=table,
                    values=row
                    )
                print(">>> ROW:", row)

    def _cast(self, value: str, dtype: int):
        try:
            if dtype in [0, 1, 2]:  # SMALLINT, INT, BIGINT
                return int(value)
            elif dtype == 3:  # DOUBLE
                print(">>> CASTING TO DOUBLE")
                return float(value)
            else:
                return value
        except:
            return value
