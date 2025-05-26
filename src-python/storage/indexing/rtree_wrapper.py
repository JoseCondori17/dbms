from rtree import index
from typing import Tuple, List

class RTree:
    def __init__(self, filename: str = None):
        p = index.Property()
        p.dimension = 2
        if filename:
            self.idx = index.Index(str(filename), properties=p)
        else:
            self.idx = index.Index(properties=p)

    def insert_point(self, obj_id: int, coords: Tuple[float, float]):
        x, y = coords
        self.idx.insert(obj_id, (x, y, x, y))

    def insert_rect(self, obj_id: int, rect: Tuple[float, float, float, float]):
        self.idx.insert(obj_id, rect)

    def range_query(self, rect: Tuple[float, float, float, float]) -> List[int]:
        return list(self.idx.intersection(rect))

    def knn_query(self, coords: Tuple[float, float], k: int = 1) -> List[int]:
        x, y = coords
        return list(self.idx.nearest((x, y, x, y), k))

    def clear(self):
        self.idx = index.Index()


# if __name__ == "__main__":
#     rtree = RTree()
#     rtree.insert_point(0, (10.0, 20.0))
#     rtree.insert_point(1, (12.5, 22.1))
#     rtree.insert_point(2, (30.0, 40.0))

#     resultados = rtree.range_query((9.0, 19.0, 13.0, 23.0))
#     if resultados:
#         print("Intersecciones:", resultados)
#     else:
#         print("No se encontraron intersecciones.")

#     knn = rtree.knn_query((11.0, 21.0), k=1)
#     print("Más cercano a (11,21):", knn)

