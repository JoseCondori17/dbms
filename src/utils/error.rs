use std::io;
use bincode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("Catalog Error: {0}")]
    Io(#[from] io::Error),
    #[error("Decode Error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Encode Error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
}