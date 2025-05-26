import struct
import os
from typing import List, Tuple, Optional, Dict, Any

from models.enum.data_type_enum import DataTypeTag
from storage.disk.data_serializer import DataSerializer

class TreeNode:
    def __init__(self, is_leaf: bool, parent_id: int = -1):
        self.is_leaf = is_leaf
        self.key_count = 0
        self.parent_id = parent_id
        self.keys: List[Any] = []
        
        if self.is_leaf:
            self.data_positions: List[int] = []
            self.next_leaf: int = -1
        else:
            self.pointers: List[int] = []
    
    def to_dict(self) -> Dict[str, Any]:
        node_dict = {
            'is_leaf': self.is_leaf,
            'key_count': self.key_count,
            'parent_id': self.parent_id,
            'keys': self.keys.copy()
        }
        
        if self.is_leaf:
            node_dict['data_positions'] = self.data_positions.copy()
            node_dict['next_leaf'] = self.next_leaf
        else:
            node_dict['pointers'] = self.pointers.copy()
        
        return node_dict
    
    @classmethod
    def from_dict(cls, node_dict: Dict[str, Any]) -> 'TreeNode':
        node = cls(node_dict['is_leaf'], node_dict['parent_id'])
        node.key_count = node_dict['key_count']
        node.keys = node_dict['keys'].copy()
        
        if node.is_leaf:
            node.data_positions = node_dict['data_positions'].copy()
            node.next_leaf = node_dict['next_leaf']
        else:
            node.pointers = node_dict['pointers'].copy()
        
        return node
    
    def insert_key(self, key: Any, index: int, data_position: Optional[int] = None, pointer: Optional[int] = None):
        self.keys.insert(index, key)
        self.key_count += 1
        
        if self.is_leaf:
            if data_position is None:
                raise ValueError("Error: leaf node")
            self.data_positions.insert(index, data_position)
        else:
            if pointer is None:
                raise ValueError("Error: internal node")
            self.pointers.insert(index + 1, pointer)
    
    def split_leaf(self) -> Tuple['TreeNode', Any]:
        mid = len(self.keys) // 2
        new_node = TreeNode(True, self.parent_id)
        
        new_node.keys = self.keys[mid:]
        new_node.data_positions = self.data_positions[mid:]
        new_node.key_count = len(new_node.keys)
        new_node.next_leaf = self.next_leaf
        
        self.keys = self.keys[:mid]
        self.data_positions = self.data_positions[:mid]
        self.key_count = len(self.keys)
        
        return new_node, new_node.keys[0]
    
    def split_internal(self) -> Tuple['TreeNode', Any]:
        mid = len(self.keys) // 2
        middle_key = self.keys[mid]
        
        new_node = TreeNode(False, self.parent_id)
        new_node.keys = self.keys[mid + 1:]
        new_node.pointers = self.pointers[mid + 1:]
        new_node.key_count = len(new_node.keys)
        
        self.keys = self.keys[:mid]
        self.pointers = self.pointers[:mid + 1]
        self.key_count = len(self.keys)
        
        return new_node, middle_key
    
    def is_underflow(self, min_keys: int) -> bool:
        return self.key_count < min_keys
    
    def is_overflow(self, max_keys: int) -> bool:
        return self.key_count > max_keys

class BPlusTreeFile:
    HEADER_SIZE = 24  # root_node_id, node_count, height, record_count, data_type, max_key_len
    NODE_HEADER_SIZE = 12
    
    def __init__(self, index_filename: str, data_type: DataTypeTag, max_key_len: int = 0, order: int = 4) -> None:
        self.index_filename = index_filename
        self.data_type = data_type
        self.max_key_len = max_key_len
        self.order = order
        self.max_keys = order - 1
        self.min_keys = max(1, (order - 1) // 2)
        
        self.key_size = DataSerializer.get_size(data_type, max_key_len)
        self.pointer_size = 4
        
        self.internal_node_size = (self.NODE_HEADER_SIZE + 
                                 (self.max_keys * self.key_size) + 
                                 (self.order * self.pointer_size))
        
        self.leaf_node_size = (self.NODE_HEADER_SIZE + 
                              (self.max_keys * self.key_size) + 
                              (self.max_keys * self.pointer_size) + 
                              self.pointer_size)
        
        self.node_size = max(self.internal_node_size, self.leaf_node_size)
        
        self.root_node_id = -1
        self.node_count = 0
        self.height = 0
        self.record_count = 0
        
        if self._is_file_empty():
            self._initialize_index_file()
        else:
            self._load_header()
    
    def insert(self, key: Any, data_position: int) -> bool:
        if self.root_node_id == -1:
            self._create_root_leaf(key, data_position)
            return True
        
        leaf_node_id = self._find_leaf(key)
        if leaf_node_id == -1:
            return False
            
        leaf_node = self._read_node(leaf_node_id)
        
        for i, existing_key in enumerate(leaf_node.keys):
            if existing_key == key:
                leaf_node.data_positions[i] = data_position
                self._write_node(leaf_node_id, leaf_node)
                return True
        
        insert_pos = 0
        for i, existing_key in enumerate(leaf_node.keys):
            if key < existing_key:
                break
            insert_pos = i + 1
        
        leaf_node.insert_key(key, insert_pos, data_position=data_position)
        
        if not leaf_node.is_overflow(self.max_keys):
            self._write_node(leaf_node_id, leaf_node)
            self.record_count += 1
            self._save_header()
            return True
        
        self._split_leaf(leaf_node_id, leaf_node)
        self.record_count += 1
        self._save_header()
        return True
    
    def delete(self, key: Any) -> Optional[int]:
        if self.root_node_id == -1:
            return None
        
        leaf_node_id = self._find_leaf(key)
        if leaf_node_id == -1:
            return None
            
        leaf_node = self._read_node(leaf_node_id)
        
        for i, existing_key in enumerate(leaf_node.keys):
            if existing_key == key:
                deleted_record_position = leaf_node.data_positions[i]
                
                leaf_node.keys.pop(i)
                leaf_node.data_positions.pop(i)
                leaf_node.key_count -= 1
                
                self._write_node(leaf_node_id, leaf_node)
                self.record_count -= 1
                self._save_header()
                
                if leaf_node.is_underflow(self.min_keys) and leaf_node_id != self.root_node_id:
                    self._handle_underflow(leaf_node_id)
                
                return deleted_record_position
        
        return None
    
    def search(self, key: Any) -> Optional[int]:
        if self.root_node_id == -1:
            return None
        
        leaf_node_id = self._find_leaf(key)
        if leaf_node_id == -1:
            return None
            
        leaf_node = self._read_node(leaf_node_id)
        
        for i, existing_key in enumerate(leaf_node.keys):
            if existing_key == key:
                return leaf_node.data_positions[i]
        
        return None
    
    def all_tuples(self) -> List[Tuple[Any, int]]:
        if self.root_node_id == -1:
            return []

        current_node_id = self.root_node_id
        while True:
            node = self._read_node(current_node_id)
            if node.is_leaf:
                break
            current_node_id = node.pointers[0] if node.pointers else -1
            if current_node_id == -1:
                return []

        result = []
        while current_node_id != -1:
            node = self._read_node(current_node_id)
            for key, data_pos in zip(node.keys, node.data_positions):
                result.append((key, data_pos))
            current_node_id = node.next_leaf

        return result

    def _initialize_index_file(self):
        with open(self.index_filename, 'wb') as f:
            f.write(struct.pack('IIIIII', 0xFFFFFFFF, 0, 0, 0, self.data_type.value, self.max_key_len))
            self.root_node_id = -1
    
    def _load_header(self):
        with open(self.index_filename, 'rb') as f:
            header_data = f.read(self.HEADER_SIZE)
            root_id, self.node_count, self.height, self.record_count, data_type_value, self.max_key_len = struct.unpack('IIIIII', header_data)
            self.root_node_id = root_id if root_id != 0xFFFFFFFF else -1
            self.data_type = DataTypeTag(data_type_value)
    
    def _save_header(self):
        with open(self.index_filename, 'r+b') as f:
            f.seek(0)
            root_id = self.root_node_id if self.root_node_id != -1 else 0xFFFFFFFF
            f.write(struct.pack('IIIIII', root_id, self.node_count, self.height, self.record_count, self.data_type.value, self.max_key_len))
    
    def _create_root_leaf(self, key: Any, data_position: int):
        node = TreeNode(True)
        node.keys = [key]
        node.data_positions = [data_position]
        node.key_count = 1
        node.next_leaf = -1
        
        self.root_node_id = 0
        self.node_count = 1
        self.height = 1
        self._write_node(0, node)
        self._save_header()
    
    def _find_leaf(self, key: Any) -> int:
        if self.root_node_id == -1:
            return -1
        
        current_node_id = self.root_node_id
        
        while current_node_id != -1:
            node = self._read_node(current_node_id)
            
            if node.is_leaf:
                return current_node_id
            
            child_index = 0
            for i, node_key in enumerate(node.keys):
                if key < node_key:
                    break
                child_index = i + 1
            
            if child_index >= len(node.pointers) or node.pointers[child_index] == 0:
                child_index = len(node.pointers) - 1
                
            if child_index < 0 or child_index >= len(node.pointers):
                return -1
                
            current_node_id = node.pointers[child_index]
            
        return -1
    
    def _split_leaf(self, node_id: int, node: TreeNode):
        new_node, first_key = node.split_leaf()
        
        new_node_id = self.node_count
        self.node_count += 1
        
        node.next_leaf = new_node_id
        
        self._write_node(node_id, node)
        self._write_node(new_node_id, new_node)
        
        self._insert_into_parent(node_id, first_key, new_node_id)
    
    def _split_internal(self, node_id: int, node: TreeNode):
        new_node, middle_key = node.split_internal()
        
        new_node_id = self.node_count
        self.node_count += 1
        
        for pointer in new_node.pointers:
            if pointer != -1:
                child = self._read_node(pointer)
                child.parent_id = new_node_id
                self._write_node(pointer, child)
        
        self._write_node(node_id, node)
        self._write_node(new_node_id, new_node)
        
        self._insert_into_parent(node_id, middle_key, new_node_id)
    
    def _insert_into_parent(self, left_child_id: int, key: Any, right_child_id: int):
        left_node = self._read_node(left_child_id)
        
        if left_node.parent_id == -1:
            new_root = TreeNode(False)
            new_root.keys = [key]
            new_root.pointers = [left_child_id, right_child_id]
            new_root.key_count = 1
            
            root_id = self.node_count
            self.node_count += 1
            self.height += 1
            self.root_node_id = root_id
            
            left_node.parent_id = root_id
            right_node = self._read_node(right_child_id)
            right_node.parent_id = root_id
            
            self._write_node(root_id, new_root)
            self._write_node(left_child_id, left_node)
            self._write_node(right_child_id, right_node)
            return
        
        parent_id = left_node.parent_id
        parent = self._read_node(parent_id)
        
        insert_pos = 0
        for i, parent_key in enumerate(parent.keys):
            if key < parent_key:
                break
            insert_pos = i + 1
        
        parent.insert_key(key, insert_pos, pointer=right_child_id)
        
        right_node = self._read_node(right_child_id)
        right_node.parent_id = parent_id
        self._write_node(right_child_id, right_node)
        
        if not parent.is_overflow(self.max_keys):
            self._write_node(parent_id, parent)
        else:
            self._split_internal(parent_id, parent)
    
    def _handle_underflow(self, node_id: int):
        node = self._read_node(node_id)
        
        if node.parent_id == -1:
            if node.key_count == 0 and not node.is_leaf and len(node.pointers) == 1:
                self.root_node_id = node.pointers[0]
                self.height -= 1
                new_root = self._read_node(self.root_node_id)
                new_root.parent_id = -1
                self._write_node(self.root_node_id, new_root)
                self._save_header()
    
    def _read_node(self, node_id: int) -> TreeNode:
        if node_id is None or node_id < 0:
            raise ValueError(f"node_id inválido: {node_id}")
            
        with open(self.index_filename, 'rb') as f:
            f.seek(self._get_node_position(node_id))
            
            header_data = f.read(self.NODE_HEADER_SIZE)
            if len(header_data) < self.NODE_HEADER_SIZE:
                raise ValueError(f"Error: header {node_id}")
                
            is_leaf, key_count, parent_id = struct.unpack('III', header_data)
            parent_id = parent_id if parent_id != 0xFFFFFFFF else -1
            
            node = TreeNode(bool(is_leaf), parent_id)
            node.key_count = key_count
            
            for i in range(self.max_keys):
                key_data = f.read(self.key_size)
                if len(key_data) < self.key_size:
                    raise ValueError(f"Could not read the key {i} of the node {node_id}")
                if i < key_count:
                    key = DataSerializer.deserialize(key_data, self.data_type, self.max_key_len)
                    node.keys.append(key)
            
            if node.is_leaf:
                for i in range(self.max_keys):
                    pos_data = f.read(self.pointer_size)
                    if len(pos_data) < self.pointer_size:
                        raise ValueError(f"Could not read the position {i} of the node {node_id}")
                    if i < key_count:
                        data_pos = struct.unpack('I', pos_data)[0]
                        node.data_positions.append(data_pos)
                
                next_data = f.read(self.pointer_size)
                if len(next_data) < self.pointer_size:
                    raise ValueError(f"Could not read the next_leaf {node_id}")
                next_leaf = struct.unpack('I', next_data)[0]
                node.next_leaf = next_leaf if next_leaf != 0xFFFFFFFF else -1
            else:
                for i in range(self.order):
                    ptr_data = f.read(self.pointer_size)
                    if len(ptr_data) < self.pointer_size:
                        raise ValueError(f"Could not read the pointer {i} from the node {node_id}")
                    if i < key_count + 1:
                        pointer = struct.unpack('I', ptr_data)[0]
                        node.pointers.append(pointer)
            
            return node
    
    def _write_node(self, node_id: int, node: TreeNode):
        current_size = os.path.getsize(self.index_filename)
        required_size = self._get_node_position(node_id) + self.node_size
        
        if required_size > current_size:
            with open(self.index_filename, 'ab') as f:
                f.write(b'\0' * (required_size - current_size))
        
        with open(self.index_filename, 'r+b') as f:
            f.seek(self._get_node_position(node_id))
            
            parent_id = node.parent_id if node.parent_id != -1 else 0xFFFFFFFF
            f.write(struct.pack('III', int(node.is_leaf), node.key_count, parent_id))
            
            for i in range(self.max_keys):
                if i < len(node.keys):
                    key_bytes = DataSerializer.serialize(node.keys[i], self.data_type, self.max_key_len)
                else:
                    key_bytes = b'\0' * self.key_size
                f.write(key_bytes)
            
            if node.is_leaf:
                for i in range(self.max_keys):
                    if i < len(node.data_positions):
                        f.write(struct.pack('I', node.data_positions[i]))
                    else:
                        f.write(struct.pack('I', 0))
                
                next_leaf = node.next_leaf if node.next_leaf != -1 else 0xFFFFFFFF
                f.write(struct.pack('I', next_leaf))
                
                remaining = self.node_size - (self.NODE_HEADER_SIZE + 
                                           (self.max_keys * self.key_size) + 
                                           (self.max_keys * self.pointer_size) + 
                                           self.pointer_size)
                if remaining > 0:
                    f.write(b'\0' * remaining)
            else:
                for i in range(self.order):
                    if i < len(node.pointers):
                        f.write(struct.pack('I', node.pointers[i]))
                    else:
                        f.write(struct.pack('I', 0))
                
                remaining = self.node_size - (self.NODE_HEADER_SIZE + 
                                           (self.max_keys * self.key_size) + 
                                           (self.order * self.pointer_size))
                if remaining > 0:
                    f.write(b'\0' * remaining)
    
    def _get_node_position(self, node_id: int) -> int:
        if node_id is None:
            raise ValueError(f"Error: node_id")
        if not isinstance(node_id, int):
            raise TypeError(f"Error: typing-: {type(node_id)} = {node_id}")
        if node_id < 0:
            raise ValueError(f"Error: least {node_id}")
            
        return self.HEADER_SIZE + (node_id * self.node_size)
    
    def _is_file_empty(self) -> bool:
        if not os.path.exists(self.index_filename):
            with open(self.index_filename, 'wb') as f:
                pass
            return True
        return os.path.getsize(self.index_filename) == 0