from xxhash import xxh64
import struct
import os

# Reference: https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
class ExtendibleHashingFile:
    HEADER_SIZE = 12
    DIRECTORY_ENTRY_SIZE = 4
    BUCKET_HEADER_SIZE = 12

    def __init__(self, index_filename: str, max_key_size: int, bucket_size: int = 4) -> None:
        self.index_filename = index_filename
        self.bucket_size = bucket_size
        self.global_depth = 0
        self.directory_size = 1
        self.bucket_count = 1

        self.max_key_size = max_key_size
        self.index_record_size = self.max_key_size + 4

        if self._is_file_empty():
            self._initialize_index_file()
        else:
            self._load_header()

    # main functions
    def insert(self, key: str, data_position: int) -> bool:
        # key length check is not needed because key is already limited by max_key_size
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
        
        new_local_depth = local_depth + 1
        bucket1_records, bucket2_records = self._split_bucket(bucket_id, local_depth, records)
        
        new_bucket_id = self.bucket_count
        self.bucket_count += 1
        self._save_header()
        
        self._write_bucket(bucket_id, new_local_depth, bucket1_records)
        self._write_bucket(new_bucket_id, new_local_depth, bucket2_records)
        
        self._update_directory_after_split(bucket_id, new_bucket_id, new_local_depth)
        
        return self.insert(key, data_position)

    def delete(self, key: str) -> int | None:
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

    def search(self, key: str) -> int | None:
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
            f.write(struct.pack('III', self.global_depth, self.directory_size, self.bucket_count))            
            f.write(struct.pack('I', 0))
            
            self._write_bucket_at_position(f, 0, [])

    def _load_header(self):
        with open(self.index_filename, 'rb') as f:
            header_data = f.read(self.HEADER_SIZE)
            self.global_depth, self.directory_size, self.bucket_count = struct.unpack('III', header_data)

    def _save_header(self):
        with open(self.index_filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('III', self.global_depth, self.directory_size, self.bucket_count))

    def _read_directory(self) -> list[int]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_directory_position())
            directory = []
            for _ in range(self.directory_size):
                bucket_id = struct.unpack('I', f.read(4))[0]
                directory.append(bucket_id)
            return directory
        
    def _write_directory(self, directory: list[int]):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_directory_position())
            for bucket_id in directory:
                f.write(struct.pack('I', bucket_id))

    def _pack_index_record(self, key: str, offset: int) -> bytes:
        if len(key) > self.max_key_size:
            key = key[:self.max_key_size]
        
        key_bytes = key.encode('utf-8')
        padding_pkey = key_bytes.ljust(self.max_key_size, b'\0')
        
        return padding_pkey + struct.pack('I', offset)

    #def _unpack_index_record(self, record_bytes: bytes) -> tuple[str, int]:
       # key_bytes = record_bytes[:self.max_key_size].rstrip(b'\0')
        #key = key_bytes.decode('utf-8')
        #offset = struct.unpack('I', record_bytes[self.max_key_size:])[0]
        #return key, offset
    
    def _unpack_index_record(self, record_bytes: bytes) -> tuple[str, int]:
        key_bytes = record_bytes[:self.max_key_size]

        # Solución: evitar decodificar basura
        if key_bytes[0] == 0 or key_bytes == b'\x00' * self.max_key_size:
            return "", -1  # o cualquier valor que indique entrada vacía

        try:
            key = key_bytes.rstrip(b'\x00').decode('utf-8')
        except UnicodeDecodeError:
            return "", -1  # ignora registros corruptos o vacíos

        offset = struct.unpack('I', record_bytes[self.max_key_size:])[0]
        return key, offset


    def _read_bucket(self, bucket_id: int) -> tuple[int, list[tuple[str, int]]]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_bucket_position(bucket_id))
            
            bucket_header = f.read(self.BUCKET_HEADER_SIZE)
            if len(bucket_header) < self.BUCKET_HEADER_SIZE:
                return 0, []
            
            local_depth, _, record_count = struct.unpack('III', bucket_header)
            
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

    def _hash_key(self, key: str) -> int:
        return xxh64(key.encode()).intdigest()

    def _get_bucket_index(self, key: str) -> int:
        hash_value = self._hash_key(key)
        return hash_value >> (64 - self.global_depth)

    def _get_bit(self, number: int, position: int) -> bool:
        return bool((number >> (63 - position)) & 1)

    def _split_bucket(self, bucket_id: int, local_depth: int, records: list[tuple[str, int]]) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        bucket1_records = []
        bucket2_records = []
        
        for key, offset in records:
            hash_value = self._hash_key(key)
            if self._get_bit(hash_value, local_depth):
                bucket2_records.append((key, offset))
            else:
                bucket1_records.append((key, offset))
        
        return bucket1_records, bucket2_records
    
    def _expand_directory(self):
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
        
        for i in range(len(directory)):
            if directory[i] == old_bucket_id:
                if self._get_bit(i, local_depth - 1):
                    directory[i] = new_bucket_id
        
        self._write_directory(directory)    

    def _get_bucket_position(self, bucket_id: int) -> int:
        directory_size_bytes = self.directory_size * self.DIRECTORY_ENTRY_SIZE
        buckets_start = self.HEADER_SIZE + directory_size_bytes
        bucket_size_bytes = self.BUCKET_HEADER_SIZE + (self.bucket_size * self.index_record_size)
        return buckets_start + (bucket_id * bucket_size_bytes)

    def _get_directory_position(self) -> int:
        return self.HEADER_SIZE

    def _is_file_empty(self):
        return os.path.getsize(self.index_filename) == 0
