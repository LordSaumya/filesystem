mod fs_ops;
mod fs_structs;

use clap::Parser;
use fs_ops::{get_filesystem_manager, FileSystemManager}; // Added FileSystemManager for direct use
                                                         // Removed unused: use fs_structs::FileNode;

#[derive(Parser, Debug)]
#[clap(
    name = "filesystem",
    version = "0.1.0",
    about = "A simple filesystem simulation app"
)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Upload a local file to the filesystem
    Upload {
        #[clap(long, short)]
        path: String, // Path to the local file
        #[clap(long, short)]
        alias: String, // Alias for the file in the filesystem
    },
    /// Download a file from the filesystem to the local system
    Download {
        #[clap(long, short)]
        alias: String, // Alias of the file in the filesystem
        #[clap(long, short)]
        path: String, // Path to save the downloaded file locally
    },
    /// List files stored in the filesystem
    List,
    /// Delete a file from the filesystem
    Delete {
        #[clap(long, short)]
        alias: String, // Alias of the file to delete
    },
    /// Initialize or re-initialize the filesystem (for testing/reset)
    Init,
}

fn main() {
    let cli = Cli::parse();

    // Attempt to get the filesystem manager.
    // In a real scenario, you might want to handle the Result more gracefully,
    // perhaps by initializing if loading fails and it's acceptable.
    // let fs_manager_result = get_filesystem_manager(); // This line will be removed

    match cli.command {
        Commands::Init => match FileSystemManager::init_filesystem() {
            Ok(_) => println!(
                "Filesystem initialized successfully at '{}'.",
                fs_ops::FILESYSTEM_FILENAME
            ),
            Err(e) => eprintln!("Error initializing filesystem: {}", e),
        },
        Commands::Upload { path, alias } => {
            // fs_manager_result is consumed or re-assigned here
            let fs_manager_result_for_upload = get_filesystem_manager(); // Renamed and made immutable
            match fs_manager_result_for_upload {
                Ok(mut manager) => match manager.upload_file(&path, &alias) {
                    Ok(_) => println!("File '{}' uploaded successfully as '{}'.", path, alias),
                    Err(e) => eprintln!("Error uploading file: {}", e),
                },
                Err(e) => eprintln!("Failed to access filesystem: {}", e),
            }
        }
        Commands::Download { alias, path } => {
            let fs_manager_result_for_download = get_filesystem_manager();
            match fs_manager_result_for_download {
                // Changed variable name to avoid conflict if fs_manager_result was intended to be used later
                Ok(mut manager) => {
                    // Made manager mutable here
                    match manager.download_file(&alias, &path) {
                        Ok(_) => {
                            println!("File '{}' downloaded successfully to '{}'.", alias, path)
                        }
                        Err(e) => eprintln!("Error downloading file: {}", e),
                    }
                }
                Err(e) => eprintln!("Failed to access filesystem: {}", e),
            }
        }
        Commands::List => {
            let fs_manager_result_for_list = get_filesystem_manager(); // Get a fresh instance of the manager
            match fs_manager_result_for_list {
                // Use the fresh instance
                Ok(manager) => {
                    // manager can be immutable as list_files takes &self
                    match manager.list_files() {
                        Ok(files) => {
                            if files.is_empty() {
                                println!("Filesystem is empty.");
                            } else {
                                println!("Files in filesystem:");
                                for file_info in files {
                                    println!("- {}", file_info);
                                }
                            }
                        }
                        Err(e) => eprintln!("Error listing files: {}", e),
                    }
                }
                Err(e) => eprintln!("Failed to access filesystem: {}", e),
            }
        }
        Commands::Delete { alias } => {
            // fs_manager_result is consumed or re-assigned here
            let fs_manager_result_for_delete = get_filesystem_manager(); // Renamed and made immutable
            match fs_manager_result_for_delete {
                Ok(mut manager) => match manager.delete_file(&alias) {
                    Ok(_) => println!("File '{}' deleted successfully.", alias),
                    Err(e) => eprintln!("Error deleting file: {}", e),
                },
                Err(e) => eprintln!("Failed to access filesystem: {}", e),
            }
        }
    }
}
