from pathlib import Path
from io import BufferedWriter, BufferedReader

from catalog.table import Table
from storage.disk.fixed_length import FixedLengthRecord

class HeapFile:
    def __init__(self, table: Table, file_path: Path, buf_size: int = 8192):
        self.table = table
        self.file_path = file_path
        self._file = open(file_path, "a+b", buffering=0)
        self.writer = BufferedWriter(raw=self._file, buffer_size=buf_size) #8kb
        self.reader = BufferedReader(raw=self._file, buffer_size=buf_size) #8kb

        self.fixed_length = FixedLengthRecord(table)
        self.fixed_length.set_format_str()

    def insert(self, data_tuple: tuple) -> int:
        packed_data = self.fixed_length.packing(data_tuple, is_active=True)
        self.writer.write(packed_data)
        self.writer.flush()
        offset = self.writer.tell()
        return (offset // self.fixed_length.get_format_size()) - 1

    def finalize(self):
        self.writer.flush()

    def read_at(self, offset: int) -> bytes:
        self.reader.seek(offset * self.fixed_length.get_format_size())
        return self.reader.read(self.fixed_length.get_format_size())

    def close(self):
        self.writer.flush()
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()