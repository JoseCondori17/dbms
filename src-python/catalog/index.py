from dataclasses import dataclass
from pathlib import Path

@dataclass
class Index:
    idx_id: int
    idx_type: int
    idx_name: str
    idx_file: Path
    idx_tuples: int
    idx_columns: list[int]  # column positions
    idx_is_primary: bool

    # GETTERS
    def get_idx_id(self) -> int:
        return self.idx_id

    def get_idx_name(self) -> str:
        return self.idx_name

    def get_idx_type(self) -> int:
        return self.idx_type

    def get_idx_file(self) -> Path:
        return self.idx_file

    def get_idx_tuples(self) -> int:
        return self.idx_tuples

    def get_idx_columns(self) -> list[int]:
        return self.idx_columns.copy()

    def get_idx_is_primary(self) -> bool:
        return self.idx_is_primary