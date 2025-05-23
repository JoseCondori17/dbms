from dataclasses import dataclass
from sqlglot import expressions as exp
from sqlglot.optimizer import optimizer

from catalog.catalog_manager import CatalogManager
from engine.planner import login_plan

@dataclass
class Select:
    catalog: CatalogManager
    
    def execute(self, expr: exp.Select):
        plan = login_plan(expr)
        print(plan.condition)