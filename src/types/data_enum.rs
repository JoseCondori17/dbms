#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum IndexType {
    Sequential = 1,
    AVL = 2,
    ISAM = 3,
    HashingExtensible = 4,
    BPlusTree = 5,
    RTree = 6
}