from pathlib import Path
from dataclasses import dataclass, field
from sqlglot import expressions as exp

from query.parser_sql import parser_sql
from engine.planner import login_plan
from engine.operators.create import Create
from engine.operators.insert import Insert
from engine.operators.select import Select
from catalog.catalog_manager import CatalogManager

@dataclass
class PKAdmin:
    catalog: CatalogManager = field(default_factory=lambda: CatalogManager(Path("data")))
    search_path: str = field(default="postgres/public")

    def execute(self, sql: str) -> None:
        exprs = parser_sql(sql)
        for expr in exprs:
            if isinstance(expr, exp.Set):
                # SET SEARCH PATH
                pass
            elif isinstance(expr, exp.Create):
                create = Create(self.catalog)
                create.execute(expr)
            elif isinstance(expr, exp.Insert):
                insert = Insert(self.catalog)
                insert.execute(expr)
            elif isinstance(expr, exp.Select):
                select = Select(self.catalog)
                select.execute(expr)
            elif isinstance(expr, exp.Delete):
                pass
            elif isinstance(expr, exp.Update):
                pass
            else:
                print(f"Undefined: {type(expr)}")

    def get(self):
        return self.catalog.get_tables("ecm", "store")