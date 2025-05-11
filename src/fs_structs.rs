// Struct definitions for the filesystem
use serde::{Deserialize, Serialize};

pub const KILOBYTE: usize = 1024;
pub const MEGABYTE: usize = 1024 * KILOBYTE;
pub const FILESYSTEM_SIZE: usize = MEGABYTE; // 1 MB
pub const BLOCK_SIZE: usize = 4 * KILOBYTE; // 4 KB
pub const NEXT_BLOCK_POINTER_SIZE: usize = std::mem::size_of::<usize>();
pub const USABLE_BLOCK_SIZE: usize = BLOCK_SIZE - NEXT_BLOCK_POINTER_SIZE;
pub const MAX_FILENAME_LENGTH: usize = 255; // Max length for file alias

// Placeholder for Header structure
#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub version: u32,
    pub total_size: usize,
    pub block_size: usize,
    pub filenode_table_offset: usize,
    pub filenode_table_size: usize, // Number of filenodes
    pub free_block_bitmap_offset: usize,
    pub data_blocks_offset: usize,
    pub num_data_blocks: usize,
}

use serde_big_array::BigArray;

/// FileNode structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileNode {
    #[serde(with = "BigArray")]
    pub alias: [u8; MAX_FILENAME_LENGTH],
    pub alias_len: u8, // Actual length of the alias
    pub size: usize,
    pub first_block_index: Option<usize>, // Index of the first data block
    pub is_used: bool,
}

impl FileNode {
    pub fn new() -> Self {
        FileNode {
            alias: [0; MAX_FILENAME_LENGTH],
            alias_len: 0,
            size: 0,
            first_block_index: None,
            is_used: false,
        }
    }

    pub fn get_alias_str(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.alias[0..self.alias_len as usize].to_vec())
    }
}
