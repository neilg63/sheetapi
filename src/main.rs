use axum::{
  extract::{DefaultBodyLimit, Json, Multipart, Path as PathParam}, http::{Method, StatusCode}, response::IntoResponse, routing::{get, post, put}, Router
};
use mongodb::action::StartTransaction;
use serde::{Deserialize, Serialize};
use spreadsheet_to_json::{calamine::Metadata, heck::ToKebabCase, ResultSet};
use spreadsheet_to_json::{process_spreadsheet_immediate, simple_string_patterns::ToSegments, OptionSet, ReadMode};
use std::{fs::{self, File}, os::unix::fs::MetadataExt, time};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tower_http::cors::{CorsLayer, Any};
use axum_typed_multipart::{FieldData, TryFromMultipart, TypedMultipart};
use tempfile::NamedTempFile;
use serde_json::json;



#[derive(TryFromMultipart, Debug)]
struct UploadAssetRequest {
  file: FieldData<NamedTempFile>,
  mode: Option<String>,
  max: Option<usize>,
  keys: Option<String>,
  lines: Option<usize>,
  cols: Option<String>,
  sheet_index: Option<usize>,
  header_index: Option<usize>,
}

#[derive(Serialize, Deserialize)]
struct CoreOptions {
  filename: Option<String>,
  mode: Option<String>,
  max: Option<usize>,
  keys: Option<String>,
  lines: Option<u8>,
  cols: Option<String>,
  sheet_index: Option<usize>,
  header_index: Option<usize>,
}

impl UploadAssetRequest {
  fn to_core_options(&self) -> CoreOptions {
      CoreOptions {
          filename: self.file.metadata.file_name.clone(),
          mode: self.mode.clone(),
          max: self.max,
          keys: self.keys.clone(),
          lines: self.lines.map(|l| l as u8),
          cols: self.cols.clone(),
          sheet_index: self.sheet_index,
          header_index: self.header_index,
      }
  }
}

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


async fn match_available_path_name(filename: &str) -> Option<FileInfo> {
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

fn json_error_response(message: &str) -> Json<serde_json::Value> {
  Json(json!({
      "valid": false,
      "message": message,
  }))
}

fn build_filename(file: &FieldData<NamedTempFile>) -> String {
  let file_name = file.metadata.file_name.clone().unwrap();
  let (start, end) = file_name.to_start_end(".");
  let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() % 1_000_000;
  format!("{}--{}.{}", start.to_kebab_case(), timestamp, end)
}

#[axum::debug_handler]
async fn upload_asset(
  TypedMultipart(request): TypedMultipart<UploadAssetRequest>,
) -> impl IntoResponse {
  let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
  let file_name = build_filename(&request.file);
  let file_path = Path::new(tmp_directory.as_str()).join(sub_directory.as_str()).join(&file_name);
  // Save the file to the temporary directory
  if let Ok(_fn) = ensure_directory_and_construct_path(&tmp_directory, &sub_directory, &file_name) {
    save_file(&request.file, &file_path).ok();
  } else {
    return (StatusCode::INTERNAL_SERVER_ERROR, json_error_response("Failed to create directory")).into_response();
  }
  let core_options = &request.to_core_options();
  match process_asset_common(file_path, &core_options).await {
    Ok(response) => response.into_response(),
    Err((status, message)) => (status, json_error_response(&message)).into_response(),
  }
}

#[axum::debug_handler]
async fn process_asset(
  Json(core_options): Json<CoreOptions>,
) -> impl IntoResponse {
  let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
  let file_name = core_options.filename.clone().unwrap_or(String::from("empty.ods"));
  let file_path = Path::new(tmp_directory.as_str()).join(sub_directory.as_str()).join(&file_name);
  
  match process_asset_common(file_path, &core_options).await {
    Ok(response) => response.into_response(),
    Err((status, message)) => (status, json_error_response(&message)).into_response(),
  }
}

async fn check_file(PathParam(file_name): PathParam<String>) -> impl IntoResponse {
  match match_available_path_name(&file_name).await {
    Some(info) => {
        let response = json!({
            "exists": true,
            "info": info
        });
        (StatusCode::OK, Json(response))
    }
    None => {
        let response = json!({
            "exists": false
        });
        (StatusCode::OK, Json(response))
    }
  }
}

async fn welcome() -> impl IntoResponse {
  let response = json!({
      "message": "Welcome to the Spreadsheet to JSON API",
      "routes": {
          "upload": {
              "method": "POST",
              "path": "/upload",
              "type": "multipart/form-data",
              "parameters": {
                "file": "The spreadsheet file to upload",
                "mode": "The read mode to use (sync or preview)",
                "max": "The maximum number of rows to read",
                "keys": "The keys to use for the columns",
                "lines": "The number of lines to read",
                "cols": "Column settings",
                "sheet_index": "The index of the sheet to read",
                "header_index": "The index of the header row"
              },
              "description": "Upload a spreadsheet file"
          },
          "process": {
              "method": "PUT",
              "path": "/process","type": "multipart/form-data",
              "parameters": {
                "filename": "The assigned name of the temporary file",
                "mode": "The read mode to use (sync or preview)",
                "max": "The maximum number of rows to read",
                "keys": "The keys to use for the columns",
                "lines": "The number of lines to read",
                "cols": "Column settings",
                "sheet_index": "The index of the sheet to read",
                "header_index": "The index of the header row"
              },
              "description": "Re-process an uploaded spreadsheet file with new criteria"
          },
          "check-file": {
              "method": "GET",
              "path": "/check-file/:file_name",
              "description": "Check if a file exists in the temporary directory"
          }
      }
  });
  (StatusCode::OK, Json(response))
}

async fn process_asset_common(
  file_path: PathBuf,
  core_options: &CoreOptions,
) -> Result<impl IntoResponse, (StatusCode, String)> {
  let default_limit: usize = dotenv::var("DEFAULT_LIMIT").unwrap_or(String::from("1000")).parse().unwrap_or(1000);
  let default_preview_limit = dotenv::var("DEFAULT_PREVIEW_LIMIT").unwrap_or(String::from("25")).parse().unwrap_or(25);
  let max_limit: usize = dotenv::var("MAX_LIMIT").unwrap_or(String::from("10000")).parse().unwrap_or(10000);
  let max_preview_limit = dotenv::var("MAX_PREVIEW_LIMIT").unwrap_or(String::from("50")).parse().unwrap_or(200);

  
  let mut col_values: Vec<serde_json::Value> = vec![];
	if let Some(file_name) = core_options.filename.clone() {
		let mode_key = core_options.mode.clone().unwrap_or("sync".to_string());
		let read_mode = ReadMode::from_key(&mode_key);
		let is_preview = read_mode.is_multimode();
		let max_row_count = if is_preview {
			max_preview_limit
		} else {
			max_limit
		};
		let default_row_count = if is_preview {
			default_preview_limit
		} else {
			default_limit
		};
		let head_keys = if let Some(key_str) = core_options.keys.clone() {
			key_str.to_parts(",")
		} else {
			vec![]
		};
		let limit = if let Some(max_val) = core_options.max {
			if max_val < max_row_count {
				max_val
			} else {
				max_row_count
			}
		} else {
			default_row_count
		};
		let line_mode = if let Some(line_val) = core_options.lines {
			line_val > 0
		} else {
			false
		};
    if let Some(cols_str) = core_options.cols.clone() {
      col_values = serde_json::from_str(&cols_str).unwrap_or_else(|_| vec![]);
    }
    if col_values.len() < 1 && head_keys.len() > 0 {
      col_values = head_keys.iter().map(|k| json!({ "key": k })).collect();
    }
    let top_index = core_options.header_index.unwrap_or(0);
    let h_index = if top_index < 256 {
      top_index as u8
    } else {
      0u8
    };
    let s_index = core_options.sheet_index.unwrap_or(0);
    let opts = OptionSet::new(&file_path.to_string_lossy().to_string())
        .set_read_mode(&mode_key)
        .max_row_count(limit as u32)
        .sheet_index(s_index as u32)
        .header_row(h_index)
        .override_columns(&col_values);
    match process_spreadsheet_immediate(&opts).await {
      Ok(result) => {
        /* if !is_preview {
          remove_uploaded_file(&file_path);
        } */
        let file_name_clone = file_name.clone();
        tokio::spawn(async move {
          let file_name = file_name_clone;
          // Clean-up logic here
          if let Ok((num_deleted, num_files)) = perform_cleanup(Some(&file_name)).await {
            println!("Deleted {} of {} files", num_deleted, num_files);
          }
        });
        let response = result.to_json();
        Ok(Json(response).into_response())
      }
      Err(_) => {
          remove_uploaded_file(&file_path);
          Err((StatusCode::NOT_ACCEPTABLE, "Failed to process file".to_string()))
      },
    }
  } else {
    Err((StatusCode::BAD_REQUEST, "No file name provided".to_string()))
  }
}

fn save_file(file: &FieldData<NamedTempFile>, file_path: &Path) -> Result<(), std::io::Error> {
  
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

fn remove_uploaded_file(file_path: &PathBuf) -> bool{
	if let Err(e) = std::fs::remove_file(file_path) {
		false
	} else {
		true
	}
}


fn ensure_directory_and_construct_path(
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

fn tmp_file_delete_after_seconds() -> u64 {
  dotenv::var("DELETE_TMP_FILES_AFTER_SECONDS")
      .unwrap_or_else(|_| String::from("600"))
      .parse()
      .unwrap_or(600)
}

async fn perform_cleanup(current_fn: Option<&str>) -> Result<(usize, usize), std::io::Error> {
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

fn scan_files_for_deletion(files: fs::ReadDir, delete_after_seconds: u64, current_fn: Option<&str>) -> Result<(usize, usize), std::io::Error> {
  let mut num_deleted = 0;
  let mut num_files = 0;
  let check_filename = current_fn.is_some();
  let cFname = current_fn.unwrap_or("").to_string();
  for file in files {
    if let Ok(file) = file {
        let file_path = file.path();
        if let Ok(fm) = file.metadata() {
          if fm.is_file() {
            if let Ok(modified) = fm.modified() {
              let fname = file.file_name().to_string_lossy().to_string();
              let is_current = check_filename && fname == cFname;
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

fn get_tmp_and_sub_directories() -> (String, String) {
  let tmp_dir = dotenv::var("TMP_FILE_DIR").unwrap_or(String::from("/tmp"));
  let sub_dir = dotenv::var("SPREADSHEET_SUBDIR").unwrap_or(String::from("sheets"));
  (tmp_dir, sub_dir)
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
  let cors = CorsLayer::new()
  .allow_origin(Any)
  .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE]);

  let app = Router::new()
    .route("/", get(welcome))
      .route("/upload", post(upload_asset))
      .route("/process", put(process_asset))
      .route("/check-file/:file_name", get(check_file))
      // The default axum body size limit is 2MiB, so we increase it to 1GiB.
      .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
      .layer(cors)
      .into_make_service();
  let ip = dotenv::var("LOCAL_ADDRESS").unwrap_or(String::from("0.0.0.0"));
  let port = dotenv::var("PORT").unwrap_or(String::from("3000"));
  let address = format!("{}:{}", ip, port);
  let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
  axum::serve(listener, app).await.unwrap();
  Ok(())
}
