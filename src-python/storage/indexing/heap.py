from pathlib import Path
from io import BufferedWriter, BufferedReader
from catalog.table import Table

class HeapFile:
    def __init__(self, table: Table, file_path: Path, buf_r: int = 8192, buf_w: int = 8192):
        self.table = table
        raw_file = open(file_path, "a+b")
        self.writer = BufferedWriter(raw=raw_file, buffer_size=buf_r) #8kb
        self.reader = BufferedReader(raw=raw_file, buffer_size=buf_w) #8kb
        self.file_path = file_path
        self._file = raw_file

    def insert(self, data: bytes):
        self.writer.write(data)

    def finalize(self):
        self.writer.flush()

    def read_at(self, offset: int) -> bytes:
        self.reader.seek(offset * self.record_size())
        return self.reader.read(self.record_size())

    def close(self):
        self.writer.flush()
        self._file.close()

    def record_size(self):
        size = 0
        for column in self.table.get_tab_columns():
            size += column.get_att_len()
        return size