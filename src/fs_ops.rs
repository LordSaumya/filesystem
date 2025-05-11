// Core logic for the filesystem operations.

use crate::fs_structs::{
    FileNode, Header, BLOCK_SIZE, FILESYSTEM_SIZE, MAX_FILENAME_LENGTH, NEXT_BLOCK_POINTER_SIZE,
    USABLE_BLOCK_SIZE,
};
use bincode;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const FILESYSTEM_FILENAME: &str = "myfs.dat";

/// FileSystemManager handles the filesystem operations.
pub struct FileSystemManager {
    pub file: File,
    header: Header,
    filenodes: Vec<FileNode>,
    free_block_bitmap: Vec<bool>, // In-memory: true = FREE, false = USED
}

impl FileSystemManager {
    pub fn init_filesystem() -> Result<Self, String> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(FILESYSTEM_FILENAME)
            .map_err(|e| format!("Failed to open/create {}: {}", FILESYSTEM_FILENAME, e))?;

        let metadata = file
            .metadata()
            .map_err(|e| format!("Failed to get metadata for {}: {}", FILESYSTEM_FILENAME, e))?;
        if metadata.len() < FILESYSTEM_SIZE as u64 {
            file.set_len(FILESYSTEM_SIZE as u64)
                .map_err(|e| format!("Failed to set length for {}: {}", FILESYSTEM_FILENAME, e))?;
        }

        let header_size: usize = std::mem::size_of::<Header>();
        let num_filenodes: usize = 100; // Max number of files

        // Calculate the actual on-disk size of the serialized Vec<FileNode>
        // Bincode stores length of vector as prefix (u64), and then the serialised vector.
        let serialized_filenode_table_bytes: usize =
            std::mem::size_of::<u64>() + (num_filenodes * std::mem::size_of::<FileNode>());

        // Calculate tentative offsets to determine the number of data blocks and bitmap size.
        let tentative_data_blocks_offset_for_calc: usize =
            header_size + serialized_filenode_table_bytes;
        let tentative_num_data_blocks_for_calc: usize =
            (FILESYSTEM_SIZE.saturating_sub(tentative_data_blocks_offset_for_calc)) / BLOCK_SIZE;
        let bitmap_size_bytes: usize = (tentative_num_data_blocks_for_calc + 7) / 8;

        // Calculate actual offsets based on the above calculations.
        let actual_filenode_table_offset: usize = header_size;
        let actual_free_block_bitmap_offset: usize =
            actual_filenode_table_offset + serialized_filenode_table_bytes;
        let actual_data_blocks_offset: usize = actual_free_block_bitmap_offset + bitmap_size_bytes;

        let actual_num_data_blocks: usize = if FILESYSTEM_SIZE > actual_data_blocks_offset {
            (FILESYSTEM_SIZE - actual_data_blocks_offset) / BLOCK_SIZE
        } else {
            0
        };

        if actual_num_data_blocks == 0 && FILESYSTEM_SIZE > BLOCK_SIZE {
            return Err(
                "Calculated zero data blocks. Filesystem size or offsets might be misconfigured."
                    .to_string(),
            );
        }

        // Creates the header with the calculated offsets and sizes.
        let header: Header = Header {
            version: 1,
            total_size: FILESYSTEM_SIZE,
            block_size: BLOCK_SIZE,
            filenode_table_offset: actual_filenode_table_offset,
            filenode_table_size: num_filenodes,
            free_block_bitmap_offset: actual_free_block_bitmap_offset,
            data_blocks_offset: actual_data_blocks_offset,
            num_data_blocks: actual_num_data_blocks,
        };

        // Write the header to the beginning of the file.
        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("Seek failed (header): {}", e))?;
        bincode::serialize_into(&mut file, &header)
            .map_err(|e| format!("Header serialization failed: {}", e))?;

        // Initialise filenodes (all empty/unused)
        let filenodes: Vec<FileNode> = vec![FileNode::new(); num_filenodes];
        file.seek(SeekFrom::Start(header.filenode_table_offset as u64))
            .map_err(|e| format!("Seek failed (filenodes): {}", e))?;

        // Serialise the entire Vec<FileNode>.
        bincode::serialize_into(&mut file, &filenodes)
            .map_err(|e| format!("Filenodes serialization failed: {}", e))?;

        // Write the free block bitmap (initially all blocks are free).
        let free_block_bitmap: Vec<bool> = vec![true; header.num_data_blocks];
        let disk_bitmap_bytes: Vec<u8> = vec![0; bitmap_size_bytes];
        file.seek(SeekFrom::Start(header.free_block_bitmap_offset as u64))
            .map_err(|e| format!("Seek failed (bitmap): {}", e))?;
        file.write_all(&disk_bitmap_bytes)
            .map_err(|e| format!("Bitmap write failed: {}", e))?;

        // Flush the file to ensure all data is written.
        file.flush()
            .map_err(|e| format!("Failed to flush after init: {}", e))?;

        Ok(FileSystemManager {
            file,
            header,
            filenodes,
            free_block_bitmap,
        })
    }

    fn find_free_filenode_index(&self) -> Option<usize> {
        self.filenodes.iter().position(|node| !node.is_used)
    }

    fn find_free_blocks(&self, num_blocks_needed: usize) -> Option<Vec<usize>> {
        let mut free_blocks_indices = Vec::new();
        for (index, is_free) in self.free_block_bitmap.iter().enumerate() {
            if *is_free {
                free_blocks_indices.push(index);
                if free_blocks_indices.len() == num_blocks_needed {
                    return Some(free_blocks_indices);
                }
            }
        }
        None
    }

    /// Writes the entire filenode table to disk.
    fn save_filenodes(&mut self) -> Result<(), String> {

        // Seek to the beginning of the filenode table.
        self.file
            .seek(SeekFrom::Start(self.header.filenode_table_offset as u64))
            .map_err(|e| format!("Seek failed (write_all_filenodes): {}", e))?;

        // Serialise the entire Vec<FileNode> to the file.
        bincode::serialize_into(&mut self.file, &self.filenodes)
            .map_err(|e| format!("Serialize failed (write_all_filenodes): {}", e))?;

        // Flush the file to ensure all data is written.
        self.file
            .flush()
            .map_err(|e| format!("Flush failed (write_all_filenodes): {}", e))
    }

    /// Writes the free block bitmap to disk.
    fn write_bitmap_to_disk(&mut self) -> Result<(), String> {
        // Calculate the size of the bitmap in bytes.
        let bitmap_size_bytes: usize = (self.header.num_data_blocks + 7) / 8;

        // Create a byte array to represent the bitmap.
        let mut disk_bitmap_bytes: Vec<u8> = vec![0; bitmap_size_bytes];

        // Set the bits in the byte array based on the free block bitmap.
        for i in 0..self.header.num_data_blocks {
            if !self.free_block_bitmap[i] {
                disk_bitmap_bytes[i / 8] |= 1 << (i % 8);
            }
        }

        // Seek to the offset for the free block bitmap in the file.
        self.file
            .seek(SeekFrom::Start(self.header.free_block_bitmap_offset as u64))
            .map_err(|e| format!("Seek failed (write_bitmap): {}", e))?;

        // Write the bitmap to the file.
        self.file
            .write_all(&disk_bitmap_bytes)
            .map_err(|e| format!("Write failed (write_bitmap): {}", e))?;

        // Flush the file to ensure all data is written.
        self.file
            .flush()
            .map_err(|e| format!("Flush failed (write_bitmap): {}", e))
    }

    /// Uploads a file from the local filesystem to the virtual filesystem.
    pub fn upload_file(&mut self, local_path_str: &str, alias: &str) -> Result<(), String> {
        // Check if the alias is valid
        if alias.is_empty() || alias.len() > MAX_FILENAME_LENGTH {
            return Err(format!(
                "Alias length must be 1-{} chars.",
                MAX_FILENAME_LENGTH
            ));
        }

        // Check if the alias already exists
        for node in self.filenodes.iter().filter(|n| n.is_used) {
            if node.get_alias_str().map_or(false, |a| a == alias) {
                return Err(format!("File with alias '{}' already exists.", alias));
            }
        }

        // Check if the local file exists and is a file
        let local_path = Path::new(local_path_str);
        if !local_path.exists() {
            return Err(format!("Local file '{}' does not exist.", local_path_str));
        }
        if !local_path.is_file() {
            return Err(format!("'{}' is not a file.", local_path_str));
        }

        // Check if the local file is empty
        let file_size: usize = local_path
            .metadata()
            .map_err(|e| format!("Metadata failed for '{}': {}", local_path_str, e))?
            .len() as usize;
        if file_size == 0 {
            return Err("Cannot upload empty file.".to_string());
        }

        // Check if there is enough space in the filesystem
        let free_blocks_count: usize = self.free_block_bitmap.iter().filter(|&free| *free).count();
        if file_size > free_blocks_count * USABLE_BLOCK_SIZE {
            return Err(format!(
                "Not enough total space. File size: {}, Available space: approx {} bytes.",
                file_size,
                free_blocks_count * USABLE_BLOCK_SIZE
            ));
        }

        // Find a free filenode and free blocks
        let filenode_index = self
            .find_free_filenode_index()
            .ok_or("No free filenodes available.".to_string())?;
        let num_blocks_needed = (file_size + USABLE_BLOCK_SIZE - 1) / USABLE_BLOCK_SIZE;
        if num_blocks_needed == 0 && file_size > 0 {
            return Err(
                "Calculated zero blocks for a non-empty file (internal error).".to_string(),
            );
        }
        if num_blocks_needed > free_blocks_count {
            return Err(format!(
                "Not enough free blocks. Needed: {}, Available: {}.",
                num_blocks_needed, free_blocks_count
            ));
        }

        // Find free blocks
        let block_indices = self.find_free_blocks(num_blocks_needed).ok_or(format!(
            "Could not find {} free blocks.",
            num_blocks_needed
        ))?;

        // Mark the blocks as used
        let mut local_file = File::open(local_path)
            .map_err(|e| format!("Failed to open local file '{}': {}", local_path_str, e))?;
        let mut read_buffer = vec![0u8; USABLE_BLOCK_SIZE];
        let mut bytes_remaining_to_write = file_size;

        // Read from the local file and write to the filesystem
        for i in 0..num_blocks_needed {

            // Read data for the current block
            let current_fs_block_index = block_indices[i];
            let bytes_to_read_this_iteration =
                std::cmp::min(bytes_remaining_to_write, USABLE_BLOCK_SIZE);
            let mut block_data_buffer = vec![0u8; BLOCK_SIZE];
            local_file
                .read_exact(&mut read_buffer[0..bytes_to_read_this_iteration])
                .map_err(|e| format!("Read failed from local file: {}", e))?;
            block_data_buffer[0..bytes_to_read_this_iteration]
                .copy_from_slice(&read_buffer[0..bytes_to_read_this_iteration]);

            // If this is not the last block, set the next block pointer to the next block index
            if i < num_blocks_needed - 1 {
                let next_fs_block_index = block_indices[i + 1];
                block_data_buffer[USABLE_BLOCK_SIZE..BLOCK_SIZE]
                    .copy_from_slice(&next_fs_block_index.to_le_bytes());
            } else {
                block_data_buffer[USABLE_BLOCK_SIZE..BLOCK_SIZE]
                    .copy_from_slice(&usize::MAX.to_le_bytes());
            }

            // Write the block data to the filesystem
            let disk_offset = self.header.data_blocks_offset + current_fs_block_index * BLOCK_SIZE;
            self.file
                .seek(SeekFrom::Start(disk_offset as u64))
                .map_err(|e| {
                    format!("Seek failed (data block {}): {}", current_fs_block_index, e)
                })?;
            self.file.write_all(&block_data_buffer).map_err(|e| {
                format!(
                    "Write failed (data block {}): {}",
                    current_fs_block_index, e
                )
            })?;

            // Mark the block as used in the bitmap
            self.free_block_bitmap[current_fs_block_index] = false;
            bytes_remaining_to_write -= bytes_to_read_this_iteration;
        }

        if bytes_remaining_to_write != 0 {
            return Err(format!(
                "Write error: {} bytes remaining unexpectedly.",
                bytes_remaining_to_write
            ));
        }

        // Update the filenode with the alias and size
        let filenode = &mut self.filenodes[filenode_index];
        filenode.alias_len = alias.len() as u8;
        filenode.alias[0..alias.len()].copy_from_slice(alias.as_bytes());
        filenode.size = file_size;
        filenode.first_block_index = Some(block_indices[0]);
        filenode.is_used = true;

        // Save the filenode and bitmap to disk and flush the file
        self.save_filenodes()?;
        self.write_bitmap_to_disk()?;
        self.file
            .flush()
            .map_err(|e| format!("Final flush failed (upload): {}", e))?;
        Ok(())
    }

    /// Downloads a file from the virtual filesystem to the local filesystem.
    pub fn download_file(&mut self, alias: &str, local_path_str: &str) -> Result<(), String> {
        // Find the filenode by alias (immutable borrow first)
        let filenode_to_download = self
            .filenodes
            .iter()
            .find(|node| node.is_used && node.get_alias_str().map_or(false, |a| a == alias))
            .cloned(); // Clone the found filenode to avoid borrowing issues with self.file

        // Check if the filenode exists
        let filenode =
            filenode_to_download.ok_or(format!("File with alias '{}' not found.", alias))?;

        // Check if the local path is valid
        let mut local_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(local_path_str)
            .map_err(|e| {
                format!(
                    "Failed to open/create local file '{}': {}",
                    local_path_str, e
                )
            })?;
        
        // Calculate the number of bytes to download and the starting block index
        let mut bytes_to_download = filenode.size;
        let mut current_block_opt = filenode.first_block_index;
        let mut block_data_buffer = vec![0u8; BLOCK_SIZE];

        // Read the blocks from the filesystem and write to the local file
        while let Some(current_block_index) = current_block_opt {
            
            // Check if there are no more bytes to download
            if bytes_to_download == 0 {
                break;
            }

            // Check if the block index is valid
            if current_block_index >= self.header.num_data_blocks {
                return Err(format!(
                    "Invalid block index {} for file '{}'. Corrupt.",
                    current_block_index, alias
                ));
            }

            // Read the block data from the filesystem
            let disk_offset = self.header.data_blocks_offset + current_block_index * BLOCK_SIZE;
            self.file
                .seek(SeekFrom::Start(disk_offset as u64))
                .map_err(|e| {
                    format!(
                        "Seek failed (download block {}): {}",
                        current_block_index, e
                    )
                })?;
            self.file.read_exact(&mut block_data_buffer).map_err(|e| {
                format!(
                    "Read failed (download block {}): {}",
                    current_block_index, e
                )
            })?;

            // Write the block data to the local file
            let bytes_in_this_block = std::cmp::min(bytes_to_download, USABLE_BLOCK_SIZE);
            local_file
                .write_all(&block_data_buffer[0..bytes_in_this_block])
                .map_err(|e| format!("Write failed to local file '{}': {}", local_path_str, e))?;
            bytes_to_download -= bytes_in_this_block;

            if bytes_to_download == 0 {
                break;
            }

            // Get the next block index from the block data
            let mut next_block_ptr_bytes = [0u8; NEXT_BLOCK_POINTER_SIZE];
            next_block_ptr_bytes.copy_from_slice(&block_data_buffer[USABLE_BLOCK_SIZE..BLOCK_SIZE]);
            let next_block_index = usize::from_le_bytes(next_block_ptr_bytes);
            current_block_opt = if next_block_index == usize::MAX {
                None
            } else {
                Some(next_block_index)
            };
        }

        // Check if the download was incomplete
        if bytes_to_download != 0 {
            return Err(format!(
                "File download incomplete for '{}'. {} bytes remaining. Corrupt.",
                alias, bytes_to_download
            ));
        }

        // Flush the local file to ensure all data is written
        local_file
            .flush()
            .map_err(|e| format!("Flush failed for local file '{}': {}", local_path_str, e))?;
        Ok(())
    }

    /// Lists all files in the filesystem.
    pub fn list_files(&self) -> Result<Vec<String>, String> {
        let mut active_files = Vec::new();
        for filenode in &self.filenodes {
            // Check if the filenode is used
            if filenode.is_used {
                match filenode.get_alias_str() {
                    Ok(alias_str) => {
                        // Add the alias and size to the list of active files
                        active_files.push(format!("{} ({} bytes)", alias_str, filenode.size))
                    }
                    Err(_) => active_files.push(format!(
                        "[Error reading alias for filenode, size: {}]",
                        filenode.size
                    )),
                }
            }
        }
        Ok(active_files)
    }

    /// Deletes a file from the filesystem.
    pub fn delete_file(&mut self, alias: &str) -> Result<(), String> {
        // Check if the alias is valid
        let filenode_index_opt = self
            .filenodes
            .iter()
            .position(|node| node.is_used && node.get_alias_str().map_or(false, |a| a == alias));
        let filenode_index = filenode_index_opt
            .ok_or(format!("File with alias '{}' not found to delete.", alias))?;

        // Calculate the number of blocks to free
        let mut blocks_to_free = Vec::new();
        let mut current_block_opt = self.filenodes[filenode_index].first_block_index;
        let mut block_data_buffer = vec![0u8; BLOCK_SIZE];

        // Traverse the linked list of blocks and free them
        while let Some(current_block_idx) = current_block_opt {
            // Check if the block index is valid
            if current_block_idx >= self.header.num_data_blocks {
                eprintln!(
                    "Warning: Invalid block index {} for file '{}'. Corrupt.",
                    current_block_idx, alias
                );
                break;
            }

            // Mark the block as free in the bitmap
            blocks_to_free.push(current_block_idx);
            let disk_offset = self.header.data_blocks_offset + current_block_idx * BLOCK_SIZE;
            self.file
                .seek(SeekFrom::Start(disk_offset as u64))
                .map_err(|e| format!("Seek (delete block {}): {}", current_block_idx, e))?;
            self.file
                .read_exact(&mut block_data_buffer)
                .map_err(|e| format!("Read (delete block {}): {}", current_block_idx, e))?;

            // Get the next block index from the block data
            let mut next_block_ptr_bytes = [0u8; NEXT_BLOCK_POINTER_SIZE];
            next_block_ptr_bytes.copy_from_slice(&block_data_buffer[USABLE_BLOCK_SIZE..BLOCK_SIZE]);
            let next_block_index = usize::from_le_bytes(next_block_ptr_bytes);
            current_block_opt = if next_block_index == usize::MAX {
                None
            } else {
                Some(next_block_index)
            };
        }

        // Mark the blocks as free in the bitmap
        for block_idx in &blocks_to_free {
            if *block_idx < self.free_block_bitmap.len() {
                self.free_block_bitmap[*block_idx] = true;
            } else {
                eprintln!(
                    "Warning: Tried to free out-of-bounds block {} for '{}'.",
                    block_idx, alias
                );
            }
        }

        // Clear the filenode data
        let filenode = &mut self.filenodes[filenode_index];
        filenode.is_used = false;
        filenode.size = 0;
        filenode.first_block_index = None;
        filenode.alias = [0; MAX_FILENAME_LENGTH]; // Clear alias
        filenode.alias_len = 0;

        // Save the updated filenode and bitmap to disk and flush the file
        self.save_filenodes()?;
        self.write_bitmap_to_disk()?;
        self.file
            .flush()
            .map_err(|e| format!("Final flush failed (delete): {}", e))?;
        Ok(())
    }
}

pub fn get_filesystem_manager() -> Result<FileSystemManager, String> {
    if !Path::new(FILESYSTEM_FILENAME).exists() {
        return FileSystemManager::init_filesystem();
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(FILESYSTEM_FILENAME)
        .map_err(|e| format!("Failed to open {}: {}", FILESYSTEM_FILENAME, e))?;

    let mut header_data = vec![0u8; std::mem::size_of::<Header>()];
    file.read_exact(&mut header_data)
        .map_err(|e| format!("Failed to read header data: {}", e))?;
    let header: Header = bincode::deserialize(&header_data)
        .map_err(|e| format!("Failed to deserialize header: {}", e))?;

    if header.total_size != FILESYSTEM_SIZE
        || header.block_size != BLOCK_SIZE
        || header.version != 1
    {
        eprintln!("Filesystem header mismatch or incompatible version. Re-initializing.");
        return FileSystemManager::init_filesystem();
    }

    file.seek(SeekFrom::Start(header.filenode_table_offset as u64))
        .map_err(|e| format!("Seek failed (load filenodes): {}", e))?;
    let filenodes: Vec<FileNode> = bincode::deserialize_from(&mut file)
        .map_err(|e| format!("Deserialize from stream failed (load filenodes): {}", e))?;

    if filenodes.len() != header.filenode_table_size {
        return Err(format!(
            "Filenode count mismatch after deserialize. Header: {}, Actual: {}. Re-initializing.",
            header.filenode_table_size,
            filenodes.len()
        ));
    }

    let bitmap_size_bytes = (header.num_data_blocks + 7) / 8;
    let mut disk_bitmap_bytes = vec![0u8; bitmap_size_bytes];
    file.seek(SeekFrom::Start(header.free_block_bitmap_offset as u64))
        .map_err(|e| format!("Seek failed (load bitmap): {}", e))?;
    file.read_exact(&mut disk_bitmap_bytes)
        .map_err(|e| format!("Read failed (load bitmap): {}", e))?;

    let mut free_block_bitmap = vec![true; header.num_data_blocks];
    for i in 0..header.num_data_blocks {
        if (disk_bitmap_bytes[i / 8] >> (i % 8)) & 1 != 0 {
            free_block_bitmap[i] = false;
        }
    }

    Ok(FileSystemManager {
        file,
        header,
        filenodes,
        free_block_bitmap,
    })
}
