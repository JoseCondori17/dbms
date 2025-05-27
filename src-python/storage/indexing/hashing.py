from xxhash import xxh64
import struct
import os

from storage.disk.data_serializer import DataSerializer
from models.enum.data_type_enum import DataTypeTag

# Reference: https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
class ExtendibleHashingFile:
    HEADER_SIZE = 12
    DIRECTORY_ENTRY_SIZE = 4
    BUCKET_HEADER_SIZE = 12

    def __init__(self, index_filename: str, data_type: DataTypeTag, max_key_len: int, bucket_size: int = 4):
        self.index_filename = index_filename
        self.bucket_size = bucket_size
        self.global_depth = 0
        self.directory_size = 1
        self.bucket_count = 1
        self.key_type = data_type
        self.max_key_len = max_key_len
        self.index_record_size = DataSerializer.get_size(data_type, max_key_len) + 4

        if self._is_file_empty():
            self._initialize_index_file()
        else:
            self._load_header()

    def insert(self, key: any, data_position: int) -> bool:
        directory = self._read_directory()
        bucket_index = self._get_bucket_index(key)
        bucket_id = directory[bucket_index]
        local_depth, records = self._read_bucket(bucket_id)

        for i, (existing_key, _) in enumerate(records):
            if existing_key == key:
                records[i] = (key, data_position)
                self._write_bucket(bucket_id, local_depth, records)
                return True

        if len(records) < self.bucket_size:
            records.append((key, data_position))
            self._write_bucket(bucket_id, local_depth, records)
            return True

        if local_depth == self.global_depth:
            self._expand_directory()
            directory = self._read_directory()
            bucket_index = self._get_bucket_index(key)
            bucket_id = directory[bucket_index]
            local_depth, records = self._read_bucket(bucket_id)

        new_local_depth = local_depth + 1
        bucket1_records, bucket2_records = self._split_bucket(bucket_id, local_depth, records)

        new_bucket_id = self.bucket_count
        self.bucket_count += 1
        self._save_header()

        self._write_bucket(bucket_id, new_local_depth, bucket1_records)
        self._write_bucket(new_bucket_id, new_local_depth, bucket2_records)

        self._update_directory_after_split(bucket_id, new_bucket_id, new_local_depth)

        return self.insert(key, data_position)

    def delete(self, key: any) -> bool:
        directory = self._read_directory()
        bucket_index = self._get_bucket_index(key)
        bucket_id = directory[bucket_index]
        local_depth, records = self._read_bucket(bucket_id)

        for i, (record_key, _) in enumerate(records):
            if record_key == key:
                records.pop(i)
                self._write_bucket(bucket_id, local_depth, records)
                return True

        return False

    def search(self, key: any) -> int:
        directory = self._read_directory()
        bucket_index = self._get_bucket_index(key)
        bucket_id = directory[bucket_index]
        _, records = self._read_bucket(bucket_id)

        for record_key, data_position in records:
            if record_key == key:
                return data_position

        return None

    def _initialize_index_file(self):
        with open(self.index_filename, 'wb') as f:
            f.write(struct.pack('III', self.global_depth, self.directory_size, self.bucket_count))
            f.write(struct.pack('I', 0))
            self._write_bucket_at_position(f, 0, [])

    def _load_header(self):
        with open(self.index_filename, 'rb') as f:
            self.global_depth, self.directory_size, self.bucket_count = struct.unpack('III', f.read(self.HEADER_SIZE))

    def _save_header(self):
        with open(self.index_filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('III',
                                self.global_depth,
                                self.directory_size,
                                self.bucket_count))
    
    def _save_header_to_file(self, f):
        f.seek(0)
        f.write(struct.pack('III',
                            self.global_depth,
                            self.directory_size,
                            self.bucket_count))

    def _read_directory(self) -> list[int]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self.HEADER_SIZE)
            return [struct.unpack('I', f.read(4))[0] for _ in range(self.directory_size)]

    def _write_directory(self, directory: list[int]):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self.HEADER_SIZE)
            for bucket_id in directory:
                f.write(struct.pack('I', bucket_id))

    def _write_directory_to_file(self, f, directory: list[int]):
        f.seek(self.HEADER_SIZE)
        for bucket_id in directory:
            f.write(struct.pack('I', bucket_id))

    def _rebuild_file(self,
                      directory: list[int],
                      buckets: dict[int, tuple[int, list[tuple[any,int]]]]):
        with open(self.index_filename, 'r+b') as f:
            f.truncate(0)
            self._save_header_to_file(f)
            self._write_directory_to_file(f, directory)
            for bucket_id in range(self.bucket_count):
                local_depth, records = buckets.get(bucket_id, (0, []))
                f.seek(self._get_bucket_position(bucket_id))
                self._write_bucket_at_position(f, local_depth, records)

    def _pack_index_record(self, key: any, offset: int) -> bytes:
        key_bytes = DataSerializer.serialize(key, self.key_type, self.max_key_len)
        return key_bytes + struct.pack('I', offset)

    def _unpack_index_record(self, record_bytes: bytes) -> tuple[any, int]:
        key_size = DataSerializer.get_size(self.key_type, self.max_key_len)
        key_bytes = record_bytes[:key_size]
        offset = struct.unpack('I', record_bytes[key_size:])[0]
        key = DataSerializer.deserialize(key_bytes, self.key_type, self.max_key_len)
        return key, offset

    def _read_bucket(self, bucket_id: int) -> tuple[int, list[tuple[str, int]]]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_bucket_position(bucket_id))
            bucket_header = f.read(self.BUCKET_HEADER_SIZE)
            if len(bucket_header) < self.BUCKET_HEADER_SIZE:
                return 0, []
            local_depth, max_size, record_count = struct.unpack('III', bucket_header)
            records = []
            for _ in range(record_count):
                record_bytes = f.read(self.index_record_size)
                if len(record_bytes) < self.index_record_size:
                    break
                key, offset = self._unpack_index_record(record_bytes)
                if key:
                    records.append((key, offset))
            return local_depth, records

    def _write_bucket_at_position(self, f, local_depth: int, records: list[tuple[str, int]]):
        f.write(struct.pack('III', local_depth, self.bucket_size, len(records)))
        for key, offset in records:
            record_bytes = self._pack_index_record(key, offset)
            f.write(record_bytes)
        empty_slots = self.bucket_size - len(records)
        for _ in range(empty_slots):
            empty_record = b'\0' * self.index_record_size
            f.write(empty_record)

    def _write_bucket(self, bucket_id: int, local_depth: int, records: list[tuple[str, int]]):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_bucket_position(bucket_id))
            self._write_bucket_at_position(f, local_depth, records)

    def _get_bucket_position(self, bucket_id: int) -> int:
        directory_size_bytes = self.directory_size * self.DIRECTORY_ENTRY_SIZE
        buckets_start = self.HEADER_SIZE + directory_size_bytes
        bucket_size_bytes = self.BUCKET_HEADER_SIZE + (self.bucket_size * self.index_record_size)
        return buckets_start + (bucket_id * bucket_size_bytes)

    def _get_directory_position(self) -> int:
        return self.HEADER_SIZE

    def _hash_key(self, key: any) -> int:
        key_bytes = DataSerializer.serialize(key, self.key_type, self.max_key_len)
        return xxh64(key_bytes).intdigest()

    def _get_bucket_index(self, key: any) -> int:
        hash_value = self._hash_key(key)
        mask = (1 << self.global_depth) - 1
        return hash_value & mask

    def _get_bit(self, number: int, position: int) -> bool:
        return bool((number >> position) & 1)

    def _split_bucket(self, bucket_id: int, local_depth: int, records: list[tuple[str, int]]) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        bucket1_records = []
        bucket2_records = []
        bit_position = local_depth
        for key, offset in records:
            hash_value = self._hash_key(key)
            if self._get_bit(hash_value, bit_position):
                bucket2_records.append((key, offset))
            else:
                bucket1_records.append((key, offset))
        return bucket1_records, bucket2_records

    def _expand_directory(self):
        old_directory = self._read_directory()
        buckets = {i: self._read_bucket(i) for i in range(self.bucket_count)}

        self.global_depth += 1
        self.directory_size = 1 << self.global_depth
        new_directory = [
            old_directory[i % len(old_directory)]
            for i in range(self.directory_size)
        ]

        self._rebuild_file(new_directory, buckets)

    def _update_directory_after_split(self, old_bucket_id: int, new_bucket_id: int, local_depth: int):
        directory = self._read_directory()
        high_bit = 1 << (local_depth - 1)

        for i in range(len(directory)):
            if directory[i] == old_bucket_id and (i & high_bit):
                directory[i] = new_bucket_id

        self._write_directory(directory)

    def _is_file_empty(self) -> bool:
        if not os.path.exists(self.index_filename):
            with open(self.index_filename, 'wb') as f:
                pass
            return True
        return os.path.getsize(self.index_filename) == 0

    def debug_print_structure(self):
        print(f"Global Depth: {self.global_depth}")
        print(f"Directory Size: {self.directory_size}")
        print(f"Bucket Count: {self.bucket_count}")
        try:
            directory = self._read_directory()
            print(f"Directory: {directory}")
            for bucket_id in range(self.bucket_count):
                local_depth, records = self._read_bucket(bucket_id)
                print(f"Bucket {bucket_id}: depth={local_depth}, records={len(records)}")
                for key, pos in records:
                    print(f" {len(key)} {key} -> {pos}")
        except Exception as e:
            print(f"Error en debug: {e}")