from dataclasses import dataclass, field

@dataclass
class Schema:
    sch_id: int
    sch_name: str
    sch_db_id: int
    sch_tables: dict[str, int] = field(default_factory=dict)
    sch_functions: dict[str, int] = field(default_factory=dict)

    # GETTERS
    def get_id(self) -> int:
        return self.sch_id

    def get_name(self) -> str:
        return self.sch_name

    def get_database_id(self) -> int:
        return self.sch_db_id

    def get_tables(self) -> dict[str, int]:
        return self.sch_tables.copy()

    def get_functions(self) -> dict[str, int]:
        return self.sch_functions.copy()

    # METHODS
    def add_table(self, name: str, id: int) -> None:
        self.sch_tables[name] = id

    def delete_table(self, name: str) -> None:
        self.sch_tables.pop(name, None)

    def add_function(self, name: str, id: int) -> None:
        self.sch_functions[name] = id

    def delete_function(self, name: str) -> None:
        self.sch_functions.pop(name, None)