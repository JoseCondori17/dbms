from enum import IntEnum

class IndexType(IntEnum):
    SEQUENTIAL  = 0
    AVL         = 1
    ISAM        = 2
    HASHING     = 3
    BTREE       = 4
    RTREE       = 5