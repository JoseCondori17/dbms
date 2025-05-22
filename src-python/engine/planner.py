from sqlglot.planner import Step
from sqlglot.expressions import Expression

def login_plan(expr: Expression) -> Step:
    return Step().from_expression(expr)