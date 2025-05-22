from dataclasses import dataclass
from catalog.catalog_manager import CatalogManager
from sqlglot import expressions as exp

@dataclass
class Insert:
    catalog: CatalogManager

    def execute(self, expr: exp.Insert) -> None:
        fn = expr.args.get("kind")
        if fn == "INTO":
            pass
