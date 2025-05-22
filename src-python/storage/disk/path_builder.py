from pathlib import Path
from dataclasses import dataclass

@dataclass
class PathBuilder:
    base_dir: Path

    def get_base_dir(self) -> Path:
        return self.base_dir

    def system_dir(self, system: str) -> Path:
        return self.base_dir / system

    def database_dir(self, db_name: str) -> Path:
        return self.base_dir / f"db_{db_name}"

    def database_meta(self, db_name: str) -> Path:
        return self.database_dir(db_name) / "meta.dat"

    def schema_dir(self, db_name: str, schema_name: str) -> Path:
        return self.database_dir(db_name) / f"schema_{schema_name}"

    def schema_meta(self, db_name: str, schema_name: str) -> Path:
        return self.schema_dir(db_name, schema_name) / "meta.dat"

    def table_dir(self, db_name: str, schema_name: str, table_name: str) -> Path:
        return self.schema_dir(db_name, schema_name) / f"table_{table_name}"

    def table_data(self, db_name: str, schema_name: str, table_name: str) -> Path:
        return self.table_dir(db_name, schema_name, table_name) / "data.dat"

    def table_meta(self, db_name: str, schema_name: str, table_name: str) -> Path:
        return self.table_dir(db_name, schema_name, table_name) / "meta.dat"

    def table_index(self, db_name: str, schema_name: str, table_name: str, property: str) -> Path:
        return self.table_dir(db_name, schema_name, table_name) / f"idx_{property}_{table_name}.dat"

    def function_file(self, db_name: str, schema_name: str, table_name: str, function_name: str) -> Path:
        return self.schema_dir(db_name, schema_name) / f"fn_{function_name}_{table_name}.dat"