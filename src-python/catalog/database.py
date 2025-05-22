from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass
class Database:
    db_id: int
    db_name: str
    db_schemas: dict[str, int] = field(default_factory=dict)  # Maps schema name to table id
    db_created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # GETTERS
    def get_id(self) -> int:
        return self.db_id
    def get_name(self) -> str:
        return self.db_name
    def get_schemas(self) -> dict[str, int]:
        return self.db_schemas
    def get_created_at(self) -> datetime:
        return self.db_created_at
    
    # METHODS
    def add_schema(self, name: str, id: int) -> None:
        self.db_schemas[name] = id
    def delete_schema(self, name: str) -> None:
        self.db_schemas.pop(name, None)
    def get_schema_id(self, name: str) -> int | None:
        return self.db_schemas.get(name)