from enum import IntEnum

class DataTypeTag(IntEnum):
    SMALLINT    = 0
    INT         = 1
    BIGINT      = 2
    DOUBLE      = 3
    CHAR        = 4
    VARCHAR     = 5
    BOOLEAN     = 6
    UUID        = 7
    DATE        = 8
    TIME        = 9
    TIMESTAMP   = 10
    GEOMETRIC   = 11
    JSON        = 12
    DECIMAL     = 13

class DataTypeSize(IntEnum):
    SMALLINT    = 2
    INT         = 4
    BIGINT      = 8
    DOUBLE      = 8
    CHAR        = 1
    VARCHAR     = 1
    BOOLEAN     = 1
    UUID        = 16
    DATE        = 4
    TIME        = 8
    TIMESTAMP   = 8
    GEOMETRIC   = 32
    JSON        = 1024
