use std::{fs::{self, File}, os::unix::fs::MetadataExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use spreadsheet_to_json::{heck::ToKebabCase, simple_string_patterns::ToSegments};
use tokio::time::sleep;
use axum_typed_multipart::FieldData;
use tempfile::NamedTempFile;

const DEFAULT_DELETE_TMP_FILES_AFTER_SECONDS: u64 = 600;

#[derive(Serialize, Deserialize)]
pub struct FileInfo {
  filename: String,
  size: u64,
  age: u64,
}

impl FileInfo {
  pub fn new(fname: &str, size: u64, age: u64) -> Self {
    Self {
      filename: fname.to_string(),
      size,
      age,
    }
  }
    
}

pub async fn match_available_path_name(filename: &str) -> Option<FileInfo> {
    let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
    let path = Path::new(tmp_directory.as_str()).join(sub_directory.as_str()).join(filename);
    if path.exists() {
       if let Ok(metadata) = path.metadata() {
          if metadata.is_file() {
            let age = metadata.modified().unwrap().elapsed().unwrap().as_secs();
          return Some(FileInfo::new(filename, metadata.size(), age));
          }
       }
    }
    None
  }
  
  pub fn build_filename(file: &FieldData<NamedTempFile>) -> String {
    let file_name = file.metadata.file_name.clone().unwrap();
    let (start, end) = file_name.to_start_end(".");
    let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() % 1_000_000;
    format!("{}--{}.{}", start.to_kebab_case(), timestamp, end)
  }
  
  pub fn save_file(file: &FieldData<NamedTempFile>, file_path: &Path) -> Result<(), std::io::Error> {
    
    let source_path = file.contents.path();
    let mut src_file = File::open(source_path)?;
    let mut dest_file = std::fs::File::create(file_path)?;
    match std::io::copy(&mut src_file, &mut dest_file) {
      Ok(_) => {
        println!("File saved to {:?}", file_path);
      }
      Err(e) => {
        println!("Failed to save file to {:?} {}", file_path, e);
      }
    }
    Ok(())
  }
  
  pub fn remove_uploaded_file(file_path: &PathBuf) -> bool{
      if let Err(e) = std::fs::remove_file(file_path) {
          false
      } else {
          true
      }
  }
  
  
  pub fn ensure_directory_and_construct_path(
      tmp_directory: &str,
      sub: &str,
      file_name: &str,
  ) -> Result<PathBuf, std::io::Error> {
      // Construct the directory path
      let dir_path = Path::new(tmp_directory).join(sub);
  
      // Ensure the directory exists, creating it if necessary
      if !dir_path.exists() {
          fs::create_dir_all(&dir_path)?;
      }
  
      // Construct the full file path
      let file_path = dir_path.join(file_name);
  
      // Return the constructed path
      Ok(file_path)
  }

  pub fn get_tmp_and_sub_directories() -> (String, String) {
    let tmp_dir = dotenv::var("TMP_FILE_DIR").unwrap_or(String::from("/tmp"));
    let sub_dir = dotenv::var("SPREADSHEET_SUBDIR").unwrap_or(String::from("sheets"));
    (tmp_dir, sub_dir)
  }



fn tmp_file_delete_after_seconds() -> u64 {
    dotenv::var("DELETE_TMP_FILES_AFTER_SECONDS")
        .unwrap_or_else(|_| String::from("600"))
        .parse()
        .unwrap_or(DEFAULT_DELETE_TMP_FILES_AFTER_SECONDS)
  }
  
  pub async fn perform_cleanup(current_fn: Option<&str>) -> Result<(usize, usize), std::io::Error> {
    sleep(Duration::from_millis(5000)).await;
    let (tmp_dir, sub_dir) = get_tmp_and_sub_directories();
    let path = Path::new(tmp_dir.as_str()).join(sub_dir.as_str());
    let delete_after_seconds = tmp_file_delete_after_seconds();
  
    if path.exists() {
        if let Ok(files) = fs::read_dir(path) {
            return scan_files_for_deletion(files, delete_after_seconds, current_fn);
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to read temporary directory"))
  }
  
  pub fn scan_files_for_deletion(files: fs::ReadDir, delete_after_seconds: u64, current_fn: Option<&str>) -> Result<(usize, usize), std::io::Error> {
    let mut num_deleted = 0;
    let mut num_files = 0;
    let check_filename = current_fn.is_some();
    let current_filename = current_fn.unwrap_or("").to_string();
    for file in files {
      if let Ok(file) = file {
          let file_path = file.path();
          if let Ok(fm) = file.metadata() {
            if fm.is_file() {
              if let Ok(modified) = fm.modified() {
                let fname = file.file_name().to_string_lossy().to_string();
                let is_current = check_filename && fname == current_filename;
                if !is_current {
                  let now = SystemTime::now();
                  let elapsed = now.duration_since(modified).unwrap();
                  num_files += 1;
                  if elapsed.as_secs() > delete_after_seconds {
                    if let Ok(_) = std::fs::remove_file(&file_path) {
                      num_deleted += 1;
                    }
                  }
                }
              }
            }
          }
        }
    }
    Ok((num_deleted, num_files))
  }