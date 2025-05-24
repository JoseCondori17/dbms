import struct
import os

class ISAMFile:
    HEADER_SIZE = 20  # levels, block_factor, max_key_size, total_blocks, root_blocks
    BLOCK_HEADER_SIZE = 12  # level, record_count, next_overflow
    
    def __init__(self, index_filename: str, max_key_size: int, levels: int = 2, block_factor: int = 10):
        self.index_filename = index_filename
        self.max_key_size = max_key_size
        self.levels = levels
        self.block_factor = block_factor
        
        self.key_record_size = self.max_key_size + 4  # for leaf 
        self.index_record_size = self.max_key_size + 4  # for index levels
        self.record_size = max(self.key_record_size, self.index_record_size)
        
        self.block_size = self.BLOCK_HEADER_SIZE + (self.block_factor * self.record_size)
        
        self.total_blocks = 0
        self.root_blocks = 0
        
        if self._is_file_empty():
            self._initialize_file()
        else:
            self._load_header()
    
    # main functions
    def insert(self, key: str, data_position: int) -> bool:
        if len(key) > self.max_key_size:
            return False
        
        path = self._find_leaf_path(key)
        leaf_block_id = path[-1]
        
        if self._insert_in_leaf_block(leaf_block_id, key, data_position):
            return True
        
        return self._handle_leaf_overflow(leaf_block_id, key, data_position)
    
    def delete(self, key: str) -> int | None:
        path = self._find_leaf_path(key)
        leaf_block_id = path[-1]
        
        deleted_position = self._delete_from_leaf_block(leaf_block_id, key)
        if deleted_position is not None:
            return deleted_position
        
        return self._delete_from_overflow_chain(leaf_block_id, key)
    
    def search(self, key: str) -> int | None:
        path = self._find_leaf_path(key)
        leaf_block_id = path[-1]
        
        result = self._search_in_leaf_block(leaf_block_id, key)
        if result is not None:
            return result
        
        return self._search_in_overflow_chain(leaf_block_id, key)
    
    # helper functions
    def _initialize_file(self):
        with open(self.index_filename, 'wb') as f:
            self.total_blocks = 0
            
            if self.levels == 1:
                self.root_blocks = 1
                self._write_header(f)
                self._write_empty_leaf_block(f, 0)
                self.total_blocks = 1
            else:
                self.root_blocks = 1
                self._write_header(f)
                
                block_id = 0
                
                for level in range(self.levels - 1):
                    next_block_id = block_id + 1
                    if level == self.levels - 2:
                        self._write_index_block(f, level, [("", next_block_id)])
                    else:
                        self._write_index_block(f, level, [("", next_block_id)])
                    block_id += 1
                
                self._write_empty_leaf_block(f, self.levels - 1)
                self.total_blocks = self.levels
        
        self._save_header()
    
    def _find_leaf_path(self, key: str) -> list[int]:
        path = []
        current_block_id = 0
        
        for level in range(self.levels - 1):
            path.append(current_block_id)
            current_block_id = self._find_child_block(current_block_id, key, level)
        
        path.append(current_block_id)
        return path
    
    def _find_child_block(self, block_id: int, key: str, level: int) -> int:
        records, _ = self._read_index_block(block_id, level)
        if not records:
            return block_id + 1
        
        for _, (index_key, child_block) in enumerate(records):
            if not index_key or key <= index_key:
                return child_block
        
        return records[-1][1]
    
    def _read_block_header(self, block_id: int) -> tuple[int, int, int]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_block_position(block_id))
            header_data = f.read(self.BLOCK_HEADER_SIZE)
            if len(header_data) < self.BLOCK_HEADER_SIZE:
                return 0, 0, -1
            return struct.unpack('III', header_data)
    
    def _read_index_block(self, block_id: int, level: int) -> tuple[list[tuple[str, int]], int]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_block_position(block_id))            
            _, record_count, next_overflow = struct.unpack('III', f.read(self.BLOCK_HEADER_SIZE))
            
            records = []
            for _ in range(record_count):
                record_data = f.read(self.record_size)
                if len(record_data) < self.record_size:
                    break
                
                key, block_pointer = self._unpack_index_record(record_data)
                if key or block_pointer > 0:
                    records.append((key, block_pointer))
            
            return records, next_overflow
    
    def _read_leaf_block(self, block_id: int) -> tuple[list[tuple[str, int]], int]:
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_block_position(block_id))
            _, record_count, next_overflow = struct.unpack('III', f.read(self.BLOCK_HEADER_SIZE))
            
            records = []
            for _ in range(record_count):
                record_data = f.read(self.record_size)
                if len(record_data) < self.record_size:
                    break
                
                key, data_offset = self._unpack_data_record(record_data)
                if key:
                    records.append((key, data_offset))
            
            return records, next_overflow
    
    def _write_index_block(self, f, level: int, records: list[tuple[str, int]], next_overflow: int = -1):
        f.write(struct.pack('III', level, len(records), next_overflow))
        for key, block_pointer in records:
            record_data = self._pack_index_record(key, block_pointer)
            f.write(record_data)
        
        remaining = self.block_factor - len(records)
        for _ in range(remaining):
            f.write(b'\0' * self.record_size)
    
    def _write_leaf_block(self, f, level: int, records: list[tuple[str, int]], next_overflow: int = -1):
        f.write(struct.pack('III', level, len(records), next_overflow))        
        for key, data_offset in records:
            record_data = self._pack_data_record(key, data_offset)
            f.write(record_data)
        
        remaining = self.block_factor - len(records)
        for _ in range(remaining):
            f.write(b'\0' * self.record_size)
    
    def _write_empty_leaf_block(self, f, level: int):
        self._write_leaf_block(f, level, [], -1)
    
    def _insert_in_leaf_block(self, block_id: int, key: str, data_position: int) -> bool:
        records, next_overflow = self._read_leaf_block(block_id)
        
        for i, (existing_key, _) in enumerate(records):
            if existing_key == key:
                records[i] = (key, data_position)
                self._write_leaf_block_at_position(block_id, records, next_overflow)
                return True
        
        if len(records) < self.block_factor:
            records.append((key, data_position))
            records.sort(key=lambda x: x[0])
            self._write_leaf_block_at_position(block_id, records, next_overflow)
            return True
        
        return False
    
    def _write_leaf_block_at_position(self, block_id: int, records: list[tuple[str, int]], next_overflow: int = -1):
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_block_position(block_id))
            self._write_leaf_block(f, self.levels - 1, records, next_overflow)
    
    def _handle_leaf_overflow(self, leaf_block_id: int, key: str, data_position: int) -> bool:
        records, next_overflow = self._read_leaf_block(leaf_block_id)
        
        if next_overflow == -1:
            overflow_block_id = self.total_blocks
            self.total_blocks += 1
            self._save_header()
            
            with open(self.index_filename, 'r+b') as f:
                f.seek(0, 2)
                self._write_empty_leaf_block(f, self.levels - 1)
            
            self._write_leaf_block_at_position(leaf_block_id, records, overflow_block_id)
            next_overflow = overflow_block_id
        
        return self._insert_in_overflow_chain(next_overflow, key, data_position)
    
    def _insert_in_overflow_chain(self, overflow_block_id: int, key: str, data_position: int) -> bool:
        current_block = overflow_block_id
        
        while current_block != -1:
            records, next_overflow = self._read_leaf_block(current_block)
            
            for i, (existing_key, _) in enumerate(records):
                if existing_key == key:
                    records[i] = (key, data_position)
                    self._write_leaf_block_at_position(current_block, records, next_overflow)
                    return True
            
            if len(records) < self.block_factor:
                records.append((key, data_position))
                records.sort(key=lambda x: x[0])
                self._write_leaf_block_at_position(current_block, records, next_overflow)
                return True
            
            if next_overflow == -1:
                new_overflow_id = self.total_blocks
                self.total_blocks += 1
                self._save_header()
                
                with open(self.index_filename, 'r+b') as f:
                    f.seek(0, 2)
                    self._write_empty_leaf_block(f, self.levels - 1)
                
                self._write_leaf_block_at_position(current_block, records, new_overflow_id)
                
                self._write_leaf_block_at_position(new_overflow_id, [(key, data_position)], -1)
                return True
            
            current_block = next_overflow
        
        return False
    
    def _search_in_leaf_block(self, block_id: int, key: str) -> int | None:
        records, _ = self._read_leaf_block(block_id)
        
        for record_key, data_position in records:
            if record_key == key:
                return data_position
        
        return None
    
    def _search_in_overflow_chain(self, leaf_block_id: int, key: str) -> int | None:
        _, next_overflow = self._read_leaf_block(leaf_block_id)
        
        current_block = next_overflow
        while current_block != -1:
            records, next_overflow = self._read_leaf_block(current_block)
            
            for record_key, data_position in records:
                if record_key == key:
                    return data_position
            
            current_block = next_overflow
        
        return None
    
    def _delete_from_leaf_block(self, block_id: int, key: str) -> int | None:
        records, next_overflow = self._read_leaf_block(block_id)
        
        for i, (record_key, data_position) in enumerate(records):
            if record_key == key:
                records.pop(i)
                self._write_leaf_block_at_position(block_id, records, next_overflow)
                return data_position
        
        return None
    
    def _delete_from_overflow_chain(self, leaf_block_id: int, key: str) -> int | None:
        _, next_overflow = self._read_leaf_block(leaf_block_id)
        current_block = next_overflow
        while current_block != -1:
            records, next_overflow = self._read_leaf_block(current_block)
            
            for i, (record_key, data_position) in enumerate(records):
                if record_key == key:
                    records.pop(i)
                    self._write_leaf_block_at_position(current_block, records, next_overflow)
                    return data_position
                
            current_block = next_overflow
        
        return None
    
    def _pack_index_record(self, key: str, block_pointer: int) -> bytes:
        key_bytes = key.encode('utf-8') if key else b''
        if len(key_bytes) > self.max_key_size:
            key_bytes = key_bytes[:self.max_key_size]
        
        padded_key = key_bytes.ljust(self.max_key_size, b'\0')
        return padded_key + struct.pack('I', block_pointer)
    
    def _unpack_index_record(self, record_bytes: bytes) -> tuple[str, int]:
        key_bytes = record_bytes[:self.max_key_size].rstrip(b'\0')
        key = key_bytes.decode('utf-8') if key_bytes else ""
        block_pointer = struct.unpack('I', record_bytes[self.max_key_size:])[0]
        return key, block_pointer
    
    def _pack_data_record(self, key: str, data_offset: int) -> bytes:
        key_bytes = key.encode('utf-8')
        if len(key_bytes) > self.max_key_size:
            key_bytes = key_bytes[:self.max_key_size]
        
        padded_key = key_bytes.ljust(self.max_key_size, b'\0')
        return padded_key + struct.pack('I', data_offset)
    
    def _unpack_data_record(self, record_bytes: bytes) -> tuple[str, int]:
        key_bytes = record_bytes[:self.max_key_size].rstrip(b'\0')
        key = key_bytes.decode('utf-8')
        data_offset = struct.unpack('I', record_bytes[self.max_key_size:])[0]
        return key, data_offset
    
    def _write_header(self, f):
        f.write(struct.pack('IIIII', self.levels, self.block_factor, self.max_key_size, self.total_blocks, self.root_blocks))
    
    def _load_header(self):
        with open(self.index_filename, 'rb') as f:
            header_data = f.read(self.HEADER_SIZE)
            (self.levels, self.block_factor, self.max_key_size, 
             self.total_blocks, self.root_blocks) = struct.unpack('IIIII', header_data)
    
    def _save_header(self):
        with open(self.index_filename, 'r+b') as f:
            f.seek(0)
            self._write_header(f)
    
    def _get_block_position(self, block_id: int) -> int:
        return self.HEADER_SIZE + (block_id * self.block_size)
                                   
    def _is_file_empty(self) -> bool:
        return os.path.getsize(self.index_filename) == 0