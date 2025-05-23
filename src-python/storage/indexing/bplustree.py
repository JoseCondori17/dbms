import bisect
import os
import pickle

ORDER = 4  # Grado del árbol

class LeafNode:
    def __init__(self):
        self.keys = []
        self.offsets = []
        self.next = None

    def is_full(self):
        return len(self.keys) >= ORDER

class InternalNode:
    def __init__(self):
        self.keys = []
        self.children = []

    def is_full(self):
        return len(self.children) > ORDER

class BPlusTreeFile:
    def __init__(self, filename):
        self.root = LeafNode()
        self.filename = filename
        # Asegúrate de crear el archivo si no existe
        open(self.filename, 'ab').close()

    def _write_to_file(self, value):
        with open(self.filename, 'ab') as f:
            offset = f.tell()
            data = pickle.dumps(value)
            f.write(data)
        return offset

    def _read_from_file(self, offset):
        with open(self.filename, 'rb') as f:
            f.seek(offset)
            return pickle.load(f)

    def insert(self, key, value):
        offset = self._write_to_file(value)
        new_key, new_node = self._insert_recursive(self.root, key, offset)
        if new_node:
            new_root = InternalNode()
            new_root.keys = [new_key]
            new_root.children = [self.root, new_node]
            self.root = new_root

    def _insert_recursive(self, node, key, offset):
        if isinstance(node, LeafNode):
            idx = bisect.bisect_left(node.keys, key)
            if idx < len(node.keys) and node.keys[idx] == key:
                node.offsets[idx] = offset
            else:
                node.keys.insert(idx, key)
                node.offsets.insert(idx, offset)
            if node.is_full():
                return self._split_leaf(node)
            return None, None
        else:
            idx = bisect.bisect_right(node.keys, key)
            new_key, new_child = self._insert_recursive(node.children[idx], key, offset)
            if new_child:
                node.keys.insert(idx, new_key)
                node.children.insert(idx + 1, new_child)
                if node.is_full():
                    return self._split_internal(node)
            return None, None

    def _split_leaf(self, node):
        mid = len(node.keys) // 2
        new_node = LeafNode()
        new_node.keys = node.keys[mid:]
        new_node.offsets = node.offsets[mid:]
        node.keys = node.keys[:mid]
        node.offsets = node.offsets[:mid]
        new_node.next = node.next
        node.next = new_node
        return new_node.keys[0], new_node

    def _split_internal(self, node):
        mid = len(node.keys) // 2
        new_node = InternalNode()
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        up_key = node.keys[mid]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        return up_key, new_node

    def search(self, key):
        node = self.root
        while isinstance(node, InternalNode):
            idx = bisect.bisect_right(node.keys, key)
            node = node.children[idx]
        idx = bisect.bisect_left(node.keys, key)
        if idx < len(node.keys) and node.keys[idx] == key:
            offset = node.offsets[idx]
            return self._read_from_file(offset)
        return None

    def range_search(self, start, end):
        result = []
        node = self.root
        while isinstance(node, InternalNode):
            node = node.children[0]
        while node:
            for k, offset in zip(node.keys, node.offsets):
                if start <= k <= end:
                    result.append((k, self._read_from_file(offset)))
            node = node.next
        return result
