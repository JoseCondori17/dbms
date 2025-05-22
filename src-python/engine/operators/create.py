from dataclasses import dataclass
from sqlglot import expressions as exp
from catalog.catalog_manager import CatalogManager
from catalog.index import Index
from models.enum.index_enum import IndexType
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_columns,
    get_name,
)

@dataclass
class Create:
    catalog: CatalogManager

    def execute(self, expr: exp.Create) -> None:
        fn = expr.args.get("kind")
        if fn == "TABLE":
            # por default debe crear el indice BTREE
            db_name: str = get_table_catalog(expr),
            schema_name: str = get_table_schema(expr)
            table_name: str = get_table_name(expr)
            index_default_name = "pk"
            index_path = self.catalog.path_builder.table_index(db_name[0], schema_name, table_name, index_default_name)
            # PENDING: crea un archivo demas verificar
            index_default = Index(
                idx_id=self.catalog.generate_function_id(db_name[0], schema_name),
                idx_type=IndexType.BTREE.value,
                idx_name=index_default_name,
                idx_file=index_path,
                idx_tuples=0,
                idx_columns=[0],
                idx_is_primary=True,
            )
            self.catalog.create_table(
                db_name[0],
                schema_name,
                table_name,
                columns=get_columns(expr),
                indexes=[index_default,],
            )
            self.catalog.create_index(
                db_name[0],
                schema_name,
                table_name,
                index_default_name
            )
        elif fn == "INDEX":
            pass
        elif fn == "SCHEMA":
            self.catalog.create_schema(
                db_name=get_table_catalog(expr), 
                schema_name=get_table_schema(expr)
            )
        elif fn == "DATABASE":
            self.catalog.create_database(db_name=get_name(expr))
        else:
            raise ValueError(f"Unknown create kind: {fn}")