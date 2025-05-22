import os
import pickle
from pathlib import Path
from dataclasses import dataclass
from typing import Any

@dataclass
class FileManager:
    base_path: Path

    def create_file(self, path: Path) -> None:
        #path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            pass  # Create an empty file

    def open_file(self, path: Path, read_only: bool):
        mode = 'rb' if read_only else 'w+b'
        return open(path, mode)

    def delete_file(self, path: Path) -> None:
        os.remove(path)

    def create_directory(self, path: Path) -> None:
        os.makedirs(path, exist_ok=True)

    def delete_directory(self, path: Path) -> None:
        os.rmdir(path)

    def path_exists(self, path: Path) -> bool:
        full_path = self.base_path / path
        return full_path.exists()

    def read_data(self, path: Path) -> Any:
        with self.open_file(path, True) as file:
            return pickle.load(file)

    def write_data(self, data: Any, path: Path) -> None:
        with self.open_file(path, False) as file:
            pickle.dump(data, file)