import pickle
import os
from typing import List, Tuple, Optional

# Definimos el tipo Rectangle como una tupla (xmin, ymin, xmax, ymax)
Rectangle = Tuple[float, float, float, float]


class RTreeNode:
    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.entries: List[Tuple[Rectangle, int]] = []  # (rect, record_id) si hoja
        self.children: List["RTreeNode"] = []  # hijos si no es hoja

    def compute_mbr(self) -> Rectangle:
        """Computa el MBR (rectángulo mínimo envolvente) del nodo"""
        rects = [entry[0] for entry in self.entries]
        if not rects:
            return (0, 0, 0, 0)
        xmin = min(r[0] for r in rects)
        ymin = min(r[1] for r in rects)
        xmax = max(r[2] for r in rects)
        ymax = max(r[3] for r in rects)
        return (xmin, ymin, xmax, ymax)


class RTreeFile:
    def __init__(self, index_filename: str, max_entries: int = 20):
        self.index_filename = index_filename
        self.max_entries = max_entries

        if os.path.exists(index_filename) and os.path.getsize(index_filename) > 0:
            self._load()
        else:
            self.root = RTreeNode(is_leaf=True)
            self._save()

    def insert(self, rect: Rectangle, record_id: int) -> None:
        self._insert(self.root, rect, record_id)
        self._save()

    def _insert(self, node: RTreeNode, rect: Rectangle, record_id: int) -> None:
        if node.is_leaf:
            node.entries.append((rect, record_id))
            if len(node.entries) > self.max_entries:
                self._split(node)
        else:
            best_child = self._choose_subtree(node, rect)
            self._insert(best_child, rect, record_id)

    def _choose_subtree(self, node: RTreeNode, rect: Rectangle) -> RTreeNode:
        """Escoge el hijo cuyo MBR necesita menos expansión"""
        min_enlargement = float('inf')
        best_child = None
        for child in node.children:
            current_mbr = child.compute_mbr()
            enlargement = self._area(self._merge_mbr(current_mbr, rect)) - self._area(current_mbr)
            if enlargement < min_enlargement:
                min_enlargement = enlargement
                best_child = child
        return best_child

    def _split(self, node: RTreeNode) -> None:
        mid = len(node.entries) // 2
        right = RTreeNode(is_leaf=node.is_leaf)
        right.entries = node.entries[mid:]
        node.entries = node.entries[:mid]

        if node == self.root:
            new_root = RTreeNode(is_leaf=False)
            new_root.children = [node, right]
            new_root.entries = [(child.compute_mbr(), 0) for child in new_root.children]
            self.root = new_root
        else:
            raise NotImplementedError("Splits internos no implementados.")

    def search(self, rect: Rectangle) -> List[int]:
        return self._search(self.root, rect)

    def _search(self, node: RTreeNode, rect: Rectangle) -> List[int]:
        result = []
        if node.is_leaf:
            for r, rid in node.entries:
                if self._intersects(r, rect):
                    result.append(rid)
        else:
            for child in node.children:
                if self._intersects(child.compute_mbr(), rect):
                    result.extend(self._search(child, rect))
        return result

    def _intersects(self, r1: Rectangle, r2: Rectangle) -> bool:
        return not (r1[2] < r2[0] or r1[0] > r2[2] or r1[3] < r2[1] or r1[1] > r2[3])

    def _merge_mbr(self, r1: Rectangle, r2: Rectangle) -> Rectangle:
        return (
            min(r1[0], r2[0]),
            min(r1[1], r2[1]),
            max(r1[2], r2[2]),
            max(r1[3], r2[3])
        )

    def _area(self, rect: Rectangle) -> float:
        return (rect[2] - rect[0]) * (rect[3] - rect[1])

    def _save(self):
        with open(self.index_filename, 'wb') as f:
            pickle.dump(self.root, f)

    def _load(self):
        with open(self.index_filename, 'rb') as f:
            self.root = pickle.load(f)
