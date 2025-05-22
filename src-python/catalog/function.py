from dataclasses import dataclass
from pathlib import Path

@dataclass
class Function:
    fn_id: int
    fn_name: str
    fn_type: str
    fn_file_path: Path