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
            i = bisect.bisect_left(node.keys, key)
            if i < len(node.keys) and node.keys[i] == key:
                node.offsets[i].append(offset)  # agregar offset a la lista existente
            else:
                node.keys.insert(i, key)
                node.offsets.insert(i, [offset]) 
            if node.is_full():
                return self._split_leaf(node)
            return None, None
        else:
            i = bisect.bisect_right(node.keys, key)
            new_child = self._insert_recursive(node.children[i], key, offset)
            if new_child:
                new_key, child_node = new_child
                node.keys.insert(i, new_key)
                node.children.insert(i + 1, child_node)
                if len(node.keys) > ORDER:
                    return self._split_internal(node)

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
            i = bisect.bisect_right(node.keys, key)
            node = node.children[i]
        for i, k in enumerate(node.keys):
            if k == key:
                return node.offsets[i]  # ahora devuelve una lista de offsets
        return []


    def range_search_offsets(self, start, end):
        result = []
        node = self.root
        while isinstance(node, InternalNode):
            node = node.children[0]
        while node:
            for k, offset_list in zip(node.keys, node.offsets):
                if start <= k <= end:
                    for offset in offset_list:
                        result.append((k, self._read_from_file(offset)))
            node = node.next
        return result

    def range_search(self, begin, end):
        result = []
        node = self.root
        # ir al primer nodo hoja que podría contener 'begin'
        while isinstance(node, InternalNode):
            i = bisect.bisect_right(node.keys, begin)
            node = node.children[i]
        # recorrer hojas mientras la clave sea <= end
        while node:
            for i, key in enumerate(node.keys):
                if begin <= key <= end:
                    for off in node.offsets[i]:
                        result.append(self._read_from_file(off))
                elif key > end:
                    return result
            node = node.next
        return result

    def delete(self, key, offset=None):
        self._delete_recursive(self.root, key, offset)

    def _delete_recursive(self, node, key, offset):
        if isinstance(node, LeafNode):
            for i, k in enumerate(node.keys):
                if k == key:
                    if offset is None:
                        # Eliminar toda la clave y todos sus offsets
                        node.keys.pop(i)
                        node.offsets.pop(i)
                    else:
                        try:
                            node.offsets[i].remove(offset)
                            if not node.offsets[i]:
                                # si la lista queda vacía, elimina la clave
                                node.keys.pop(i)
                                node.offsets.pop(i)
                        except ValueError:
                            pass  # el offset no está en la lista
                    return
        else:
            i = bisect.bisect_right(node.keys, key)
            self._delete_recursive(node.children[i], key, offset)
            # no hacemos balancing por simplicidad, pero se puede agregar
            if isinstance(node.children[i], LeafNode):
                if len(node.children[i].keys) < (ORDER + 1) // 2:
                    self._rebalance_leaf(node, i)            
            else:
                self._rebalance_internal(node, i)

    def _rebalance_leaf(self, parent, idx):
        child = parent.children[idx]
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            if len(left_sibling.keys) > (ORDER + 1) // 2:
                # Redistribuir desde la izquierda
                child.keys.insert(0, left_sibling.keys.pop())
                child.offsets.insert(0, left_sibling.offsets.pop())
                parent.keys[idx - 1] = child.keys[0]
                return
            else:
                # Fusionar con izquierda
                left_sibling.keys += child.keys
                left_sibling.offsets += child.offsets
                left_sibling.next = child.next
                parent.keys.pop(idx - 1)
                parent.children.pop(idx)
        elif idx + 1 < len(parent.children):
            right_sibling = parent.children[idx + 1]
            if len(right_sibling.keys) > (ORDER + 1) // 2:
                # Redistribuir desde la derecha
                child.keys.append(right_sibling.keys.pop(0))
                child.offsets.append(right_sibling.offsets.pop(0))
                parent.keys[idx] = right_sibling.keys[0]
                return
            else:
                # Fusionar con derecha
                child.keys += right_sibling.keys
                child.offsets += right_sibling.offsets
                child.next = right_sibling.next
                parent.keys.pop(idx)
                parent.children.pop(idx + 1)

    def _rebalance_internal(self, parent, idx):
        child = parent.children[idx]
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            if len(left_sibling.children) > (ORDER + 1) // 2:
                # Rotación desde la izquierda
                sep_key = parent.keys[idx - 1]
                child.children.insert(0, left_sibling.children.pop())
                child.keys.insert(0, sep_key)
                parent.keys[idx - 1] = left_sibling.keys.pop()
                return
            else:
                # Fusión con izquierda
                sep_key = parent.keys.pop(idx - 1)
                left_sibling.keys.append(sep_key)
                left_sibling.keys += child.keys
                left_sibling.children += child.children
                parent.children.pop(idx)
        elif idx + 1 < len(parent.children):
            right_sibling = parent.children[idx + 1]
            if len(right_sibling.children) > (ORDER + 1) // 2:
                # Rotación desde la derecha
                sep_key = parent.keys[idx]
                child.children.append(right_sibling.children.pop(0))
                child.keys.append(sep_key)
                parent.keys[idx] = right_sibling.keys.pop(0)
                return
            else:
                # Fusión con derecha
                sep_key = parent.keys.pop(idx)
                child.keys.append(sep_key)
                child.keys += right_sibling.keys
                child.children += right_sibling.children
                parent.children.pop(idx + 1)
    
    def save_index(self,filename):
        with open(filename, 'wb') as f:
            pickle.dump(self,f)
    
    @staticmethod
    def load_index(filename):
        with open(filename,'rb') as f:
            return pickle.load(f)