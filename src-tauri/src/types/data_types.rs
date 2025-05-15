#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum DataTypeTag {
    SMALLINT    = 1,
    INT         = 2,
    BIGINT      = 3,
    DOUBLE      = 4,
    CHAR        = 5,
    VARCHAR     = 6,
    BOOLEAN     = 7,
    UUID        = 8,
    DATE        = 9,
    TIME        = 10,
    TIMESTAMP   = 11, // WITH TIME ZONE 
    GEOMETRIC   = 12,
}