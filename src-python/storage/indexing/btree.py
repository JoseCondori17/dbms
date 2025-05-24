import pickle
import os
from typing import Any, List


class BTreeNode:
    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.children: List[Any] = []
        self.next: 'BTreeNode' = None


class BTreeFile:
    def __init__(self, index_filename: str, order: int = 4):
        self.index_filename = index_filename
        self.order = order
        self.root = BTreeNode(is_leaf=True)
        if os.path.exists(self.index_filename) and os.path.getsize(self.index_filename) > 0:
            self._load()
        else:
            self._save()

    def insert(self, key: Any, value: int) -> None:
        node = self.root
        parent_stack = []

        while not node.is_leaf:
            parent_stack.append(node)
            for i, k in enumerate(node.keys):
                if key < k:
                    node = node.children[i]
                    break
            else:
                node = node.children[-1]

        self._insert_in_leaf(node, key, value)

        if len(node.keys) >= self.order:
            self._handle_split(node, parent_stack)

        self._save()

    def _insert_in_leaf(self, node: BTreeNode, key: Any, value: int) -> None:
        if not node.keys:
            node.keys.append(key)
            node.children.append([value])
            return
        for i, k in enumerate(node.keys):
            if key == k:
                node.children[i].append(value)
                return
            elif key < k:
                node.keys.insert(i, key)
                node.children.insert(i, [value])
                return
        node.keys.append(key)
        node.children.append([value])

    def _handle_split(self, node: BTreeNode, parent_stack: List[BTreeNode]) -> None:
        right, promoted_key = self._split_node(node)

        if not parent_stack:
            new_root = BTreeNode(is_leaf=False)
            new_root.keys = [promoted_key]
            new_root.children = [node, right]
            self.root = new_root
            return

        parent = parent_stack.pop()
        index = 0
        while index < len(parent.keys) and promoted_key > parent.keys[index]:
            index += 1
        parent.keys.insert(index, promoted_key)
        parent.children.insert(index + 1, right)

        if len(parent.keys) >= self.order:
            self._handle_split(parent, parent_stack)

    def _split_node(self, node: BTreeNode) -> tuple:
        mid = len(node.keys) // 2
        if node.is_leaf:
            right = BTreeNode(is_leaf=True)
            right.keys = node.keys[mid:]
            right.children = node.children[mid:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid]

            right.next = node.next
            node.next = right

            return right, right.keys[0]
        else:
            right = BTreeNode(is_leaf=False)
            right.keys = node.keys[mid + 1:]
            right.children = node.children[mid + 1:]
            promoted_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]

            return right, promoted_key

    def search(self, key: Any) -> int | None:
        node = self.root
        while not node.is_leaf:
            for i, k in enumerate(node.keys):
                if key < k:
                    node = node.children[i]
                    break
            else:
                node = node.children[-1]

        for i, k in enumerate(node.keys):
            if key == k:
                return node.children[i][0]  # solo primer resultado
        return None

    def range_search(self, start_key: Any, end_key: Any) -> List[int]:
        result = []
        node = self.root
        while not node.is_leaf:
            for i, k in enumerate(node.keys):
                if start_key < k:
                    node = node.children[i]
                    break
            else:
                node = node.children[-1]

        while node:
            for i, k in enumerate(node.keys):
                if start_key <= k <= end_key:
                    result.extend(node.children[i])
                elif k > end_key:
                    return result
            node = node.next
        return result

    def delete(self, key: Any) -> None:
        raise NotImplementedError("delete() no ha sido implementado.")

    def _save(self) -> None:
        with open(self.index_filename, 'wb') as f:
            pickle.dump(self.root, f)

    def _load(self) -> None:
        with open(self.index_filename, 'rb') as f:
            self.root = pickle.load(f)