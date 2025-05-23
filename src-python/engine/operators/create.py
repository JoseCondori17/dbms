from dataclasses import dataclass
from sqlglot import expressions as exp

from catalog.catalog_manager import CatalogManager
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
class Create:
    catalog: CatalogManager

    def execute(self, expr: exp.Create) -> None:
        fn = expr.args.get("kind")
        if fn == "TABLE":
            # default index name BTREE
            db_name: str = get_table_catalog(expr)
            schema_name: str = get_table_schema(expr)
            table_name: str = get_table_name(expr)
            index_default_name = "pk"

            self.catalog.create_table(
                db_name,
                schema_name,
                table_name,
                columns=get_columns(expr),
                indexes=[],
            )
            self.catalog.create_index(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                index_name=index_default_name,
                index_type=IndexType.BTREE,
                index_colum=0,
                index_is_primary=True,
            )
        elif fn == "INDEX":
            db_name: str = get_table_catalog(expr)
            schema_name: str = get_table_schema(expr)
            table_name: str = get_table_name(expr)
            index_name: str = get_name(expr)
            index_type: str = get_index_type(expr).upper()
            column_name: str = get_column_name(expr).lower()
            index_column = self.catalog.get_position_column_by_name(
                db_name,
                schema_name,
                table_name,
                column_name
            )
            
            self.catalog.create_index(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                index_name=index_name,
                index_type=IndexType[index_type],
                index_colum=index_column,
                index_is_primary=False,
            )
        elif fn == "SCHEMA":
            self.catalog.create_schema(
                db_name=get_table_catalog(expr), 
                schema_name=get_table_schema(expr)
            )
        elif fn == "DATABASE":
            self.catalog.create_database(db_name=get_name(expr))
        else:
            raise ValueError(f"Unknown create kind: {fn}")