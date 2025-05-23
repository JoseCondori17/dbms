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

    def delete(self, record_id: int):
        record = self.read_record(record_id)
        data_tuple, _ = record
        packed_data = self.fixed_length.packing(data_tuple, is_active=False)
        file_position = record_id * self.fixed_length.get_format_size()
        self._file.seek(file_position)
        self._file.write(packed_data)
        self._file.flush()

    def get_column_value(self, record_id: int, column_name: str) -> any:
        record = self.read_record(record_id)
        column_index = self._get_column_index(column_name)
        return record[column_index]

    def _get_column_index(self, column_name: str) -> int:
        columns = self.table.get_tab_columns()
        for index, column in enumerate(columns):
            if column.get_column_name() == column_name:
                return index

    def read_record(self, record_id: int) -> tuple[tuple[any, ...], bool]:
        data_bytes = self.read_at(record_id)
        return self.fixed_length.unpacking(data_bytes)

    def read_at(self, record_id: int) -> bytes:
        self.reader.seek(record_id * self.fixed_length.get_format_size())
        return self.reader.read(self.fixed_length.get_format_size())

    def finalize(self):
        self.writer.flush()

    def close(self):
        self.writer.flush()
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()