from xxhash import xxh64
import struct
import os

from models.enum.data_type_enum import DataTypeTag
from storage.disk.data_serializer import DataSerializer

# Reference: https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
class ExtendibleHashingFile:
    HEADER_SIZE = 20  # global_depth, directory_size, bucket_count, data_type, max_len_key
    DIRECTORY_ENTRY_SIZE = 4
    BUCKET_HEADER_SIZE = 12

    def __init__(self, index_filename: str, data_type: DataTypeTag, max_len_key: int = 0, bucket_size: int = 4) -> None:
        self.index_filename = index_filename
        self.bucket_size = bucket_size
        self.global_depth = 0
        self.directory_size = 1
        self.bucket_count = 1
        self.data_type = data_type
        self.max_len_key = max_len_key

        self.key_size = DataSerializer.get_size(data_type, max_len_key)
        self.index_record_size = self.key_size + 4

        if self._is_file_empty():
            self._initialize_index_file()
        else:
            self._load_header()

    # main functions
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
        
        if self._handle_bucket_split(key, data_position, bucket_id, local_depth, records):
            return self.insert(key, data_position)

    def _handle_bucket_split(self, key: any, data_position: int, bucket_id: int, 
                           local_depth: int, records: list[tuple[any, int]]) -> bool:
        if local_depth == self.global_depth:
            self._expand_directory()
        
        new_local_depth = local_depth + 1
        
        all_records = records + [(key, data_position)]
        
        bucket1_records, bucket2_records = self._split_records(all_records, new_local_depth)
        
        new_bucket_id = self.bucket_count
        self.bucket_count += 1
        self._save_header()
        
        self._write_bucket(bucket_id, new_local_depth, bucket1_records)
        self._write_bucket(new_bucket_id, new_local_depth, bucket2_records)
        
        self._update_directory_after_split(bucket_id, new_bucket_id, new_local_depth)
        
        return True

    def delete(self, key: any) -> int | None:
        directory = self._read_directory()
        bucket_index = self._get_bucket_index(key)
        bucket_id = directory[bucket_index]
        
        local_depth, records = self._read_bucket(bucket_id)
        
        for i, (record_key, data_position) in enumerate(records):
            if record_key == key:
                records.pop(i)
                self._write_bucket(bucket_id, local_depth, records)
                return data_position
        
        return None

    def search(self, key: any) -> int | None:
        directory = self._read_directory()
        bucket_index = self._get_bucket_index(key)
        bucket_id = directory[bucket_index]
        
        _, records = self._read_bucket(bucket_id)
        
        for record_key, data_position in records:
            if record_key == key:
                return data_position
        
        return None
    
    # helper functions
    def _initialize_index_file(self):
        with open(self.index_filename, 'wb') as f:
            f.write(struct.pack('IIIII', self.global_depth, self.directory_size, 
                              self.bucket_count, self.data_type.value, self.max_len_key))
            
            f.write(struct.pack('I', 0))
            
            self._write_bucket_at_position(f, 0, [])

    def _load_header(self):
        with open(self.index_filename, 'rb') as f:
            header_data = f.read(self.HEADER_SIZE)
            if len(header_data) < self.HEADER_SIZE:
                raise ValueError("Error: file is corrupted")
            
            self.global_depth, self.directory_size, self.bucket_count, data_type_value, self.max_len_key = struct.unpack('IIIII', header_data)
            self.data_type = DataTypeTag(data_type_value)

    def _save_header(self):
        with open(self.index_filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('IIIII', self.global_depth, self.directory_size, 
                              self.bucket_count, self.data_type.value, self.max_len_key))

    def _read_directory(self) -> list[int]:
        directory = []
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_directory_position())
            
            for i in range(self.directory_size):
                data = f.read(4)
                if len(data) < 4:
                    raise ValueError(f"Error: read record in pos {i}")
                bucket_id = struct.unpack('I', data)[0]
                directory.append(bucket_id)
            
        return directory
        
    def _write_directory(self, directory: list[int]):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_directory_position())
            for bucket_id in directory:
                f.write(struct.pack('I', bucket_id))
                

    def _pack_index_record(self, key: any, offset: int) -> bytes:
        if key is None:
            key_bytes = b'\0' * self.key_size
        else:
            key_bytes = DataSerializer.serialize(key, self.data_type, self.max_len_key)
        return key_bytes + struct.pack('I', offset)

    def _unpack_index_record(self, record_bytes: bytes) -> tuple[any, int]:
        if len(record_bytes) < self.index_record_size:
            return None, 0
            
        key_bytes = record_bytes[:self.key_size]
        offset = struct.unpack('I', record_bytes[self.key_size:self.key_size + 4])[0]
        
        if all(b == 0 for b in key_bytes):
            return None, offset
            
        key = DataSerializer.deserialize(key_bytes, self.data_type, self.max_len_key)
        return key, offset

    def _read_bucket(self, bucket_id: int) -> tuple[int, list[tuple[any, int]]]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_bucket_position(bucket_id))
            
            bucket_header = f.read(self.BUCKET_HEADER_SIZE)
            if len(bucket_header) < self.BUCKET_HEADER_SIZE:
                return 0, []
            
            local_depth, _, record_count = struct.unpack('III', bucket_header)
            
            records = []
            for i in range(record_count):
                record_bytes = f.read(self.index_record_size)
                if len(record_bytes) < self.index_record_size:
                    break
                
                key, offset = self._unpack_index_record(record_bytes)
                if key is not None:
                    records.append((key, offset))
            
            return local_depth, records

    def _write_bucket_at_position(self, f, local_depth: int, records: list[tuple[any, int]]):
        f.write(struct.pack('III', local_depth, self.bucket_size, len(records)))
        
        for key, offset in records:
            record_bytes = self._pack_index_record(key, offset)
            f.write(record_bytes)
        
        empty_slots = self.bucket_size - len(records)
        for _ in range(empty_slots):
            empty_record = b'\0' * self.index_record_size
            f.write(empty_record)

    def _write_bucket(self, bucket_id: int, local_depth: int, records: list[tuple[any, int]]):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_bucket_position(bucket_id))
            self._write_bucket_at_position(f, local_depth, records)

    def _hash_key(self, key: any) -> int:
        if key is None:
            return 0
        key_bytes = DataSerializer.serialize(key, self.data_type, self.max_len_key)
        return xxh64(key_bytes).intdigest()

    def _get_bucket_index(self, key: any) -> int:
        if self.global_depth == 0:
            return 0
        hash_value = self._hash_key(key)
        mask = (1 << self.global_depth) - 1
        return hash_value & mask

    def _get_bit(self, number: int, position: int) -> bool:
        return bool((number >> position) & 1)

    def _split_records(self, records: list[tuple[any, int]], depth: int) -> tuple[list[tuple[any, int]], list[tuple[any, int]]]:
        bucket1_records = []
        bucket2_records = []
        
        bit_position = depth - 1
        
        for key, offset in records:
            hash_value = self._hash_key(key)
            if self._get_bit(hash_value, bit_position):
                bucket2_records.append((key, offset))
            else:
                bucket1_records.append((key, offset))
        
        return bucket1_records, bucket2_records
    
    def _expand_directory(self): # duplicate entries
        directory = self._read_directory()
        new_directory = []
        
        for bucket_id in directory:
            new_directory.append(bucket_id)
            new_directory.append(bucket_id)
        
        self.global_depth += 1
        self.directory_size = len(new_directory)
        
        self._save_header()
        self._write_directory(new_directory)

    def _update_directory_after_split(self, old_bucket_id: int, new_bucket_id: int, local_depth: int):
        directory = self._read_directory()
        
        bit_position = local_depth - 1
        
        for i in range(len(directory)):
            if directory[i] == old_bucket_id:
                if self._get_bit(i, bit_position):
                    directory[i] = new_bucket_id
        
        self._write_directory(directory)

    def _get_bucket_position(self, bucket_id: int) -> int:
        directory_size_bytes = self.directory_size * self.DIRECTORY_ENTRY_SIZE
        buckets_start = self.HEADER_SIZE + directory_size_bytes
        bucket_size_bytes = self.BUCKET_HEADER_SIZE + (self.bucket_size * self.index_record_size)
        return buckets_start + (bucket_id * bucket_size_bytes)

    def _get_directory_position(self) -> int:
        return self.HEADER_SIZE

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
                    print(f"  {key} -> {pos}")
        except Exception as e:
            print(f"Error en debug: {e}")