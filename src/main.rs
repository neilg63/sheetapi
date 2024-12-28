use axum::extract::DefaultBodyLimit;
use axum::http::{Method, StatusCode};
use axum::routing::post;
use axum::Router;
use axum_typed_multipart::{FieldData, TryFromMultipart, TypedMultipart};
use redis::Value;
use serde_json::json;
use tokio::time::sleep;
use std::io::Error;
use std::time::{Duration, SystemTime};
use std::thread;
use serde::Deserialize;
use serde_with::skip_serializing_none;
use spreadsheet_to_json::simple_string_patterns::ToSegments;
use spreadsheet_to_json::{process_spreadsheet_immediate, Column, OptionSet, ReadMode};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use axum::response::IntoResponse;
use axum::Json;
use tower_http::cors::{CorsLayer, Any};

#[derive(TryFromMultipart, Debug)]
struct UploadAssetRequest {
  // The `unlimited arguments` means that this field will be limited to the
  // total size of the request body. If you want to limit the size of this
  // field to a specific value you can also specify a limit in bytes, like
  // '5MiB' or '1GiB'.
  #[form_data(limit = "unlimited")]
  file: FieldData<NamedTempFile>,

  // This field will be limited to the default size of 1MiB.
  mode: Option<String>,

	max: Option<usize>,

	keys: Option<String>,

	lines: Option<u8>,

  cols: Option<String>,

  sheet_index: Option<usize>,

  header_index: Option<usize>,
}

/* impl UploadAssetRequest {
    fn new(file: FieldData<NamedTempFile>, sub: String) -> Self {
        Self { file, sub }
    }
} */

async fn upload_asset(
    TypedMultipart(UploadAssetRequest { file , mode, max, keys, lines, cols, sheet_index, header_index }): TypedMultipart<UploadAssetRequest>,
) -> impl IntoResponse {
	let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
	let default_limit: usize = dotenv::var("DEFAULT_LIMIT").unwrap_or(String::from("1000")).parse().unwrap_or(1000);
	let default_preview_limit = dotenv::var("DEFAULT_PREVIEW_LIMIT").unwrap_or(String::from("25")).parse().unwrap_or(25);
	let max_limit: usize = dotenv::var("MAX_LIMIT").unwrap_or(String::from("10000")).parse().unwrap_or(10000);
	let max_preview_limit = dotenv::var("MAX_PREVIEW_LIMIT").unwrap_or(String::from("50")).parse().unwrap_or(200);
  let mut col_values: Vec<serde_json::Value> = vec![];
	if let Some(file_name) = file.metadata.file_name {
		let mode_key = mode.unwrap_or("sync".to_string());
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
		let head_keys = if let Some(key_str) = keys {
			key_str.to_parts(",")
		} else {
			vec![]
		};
		let limit = if let Some(max_val) = max {
			if max_val < max_row_count {
				max_val
			} else {
				max_row_count
			}
		} else {
			default_row_count
		};
		let line_mode = if let Some(line_val) = lines {
			line_val > 0
		} else {
			false
		};
    if let Some(cols_str) = cols {
      col_values = serde_json::from_str(&cols_str).unwrap_or_else(|_| vec![]);
    }
    if col_values.len() < 1 && head_keys.len() > 0 {
      col_values = head_keys.iter().map(|k| json!({ "key": k })).collect();
    }
    let top_index = header_index.unwrap_or(0);
    let h_index = if top_index < 256 {
      top_index as u8
    } else {
      0u8
    };
    let s_index = sheet_index.unwrap_or(0);
		match ensure_directory_and_construct_path(&tmp_directory, &sub_directory, &file_name) {
			Ok(path) => {
				match file.contents.persist(&path) {
					Ok(_) => {
						let file_path = path.to_string_lossy().to_string();
						let opts = OptionSet::new(&file_path)
							.set_read_mode(&mode_key)
							.max_row_count(limit as u32)
              .sheet_index(s_index as u32)
              .header_row(h_index)
              .override_columns(&col_values)
							.set_json_lines(line_mode);
						match process_spreadsheet_immediate(&opts).await {
							Ok(result) => {
									if !is_preview {
										remove_uploaded_file(&file_path);
									}
									tokio::spawn(async {
										// Clean-up logic here
										if let Ok((num_deleted, num_files)) = perform_cleanup().await {
                      println!("Deleted {} of {} files", num_deleted, num_files);
                    }
									});
									let response = result.to_json();
									Json(response).into_response()
							}
							Err(_) => {
									remove_uploaded_file(&file_path);
									(StatusCode::NOT_ACCEPTABLE, "Failed to process file").into_response()
							},
						}
					},
					Err(_) => (StatusCode::NOT_ACCEPTABLE, "Failed to persist file").into_response(),
				}
			},
			Err(err) => (StatusCode::NOT_ACCEPTABLE, format!("Failed to create directory: {}", err)).into_response(),
		}
			
	} else {
		(StatusCode::NOT_ACCEPTABLE, "Missing file name").into_response()
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

fn remove_uploaded_file(file_path: &str) -> bool{
	if let Err(e) = std::fs::remove_file(&file_path) {
		false
	} else {
		true
	}
}

fn tmp_file_delete_after_seconds() -> u64 {
  dotenv::var("DELETE_TMP_FILES_AFTER_SECONDS")
      .unwrap_or_else(|_| String::from("600"))
      .parse()
      .unwrap_or(600)
}

async fn perform_cleanup() -> Result<(usize, usize), std::io::Error> {
  sleep(Duration::from_millis(5000)).await;
  let (tmp_dir, sub_dir) = get_tmp_and_sub_directories();
  let path = Path::new(tmp_dir.as_str()).join(sub_dir.as_str());
  let delete_after_seconds = tmp_file_delete_after_seconds();

  if path.exists() {
      if let Ok(files) = fs::read_dir(path) {
          return scan_files_for_deletion(files, delete_after_seconds);
      }
  }
  Err(Error::new(std::io::ErrorKind::Other, "Failed to read temporary directory"))
}

fn scan_files_for_deletion(files: fs::ReadDir, delete_after_seconds: u64) -> Result<(usize, usize), std::io::Error> {
  let mut num_deleted = 0;
  let mut num_files = 0;
  for file in files {
    if let Ok(file) = file {
        let file_path = file.path();
        if let Ok(fm) = file.metadata() {
          if fm.is_file() {
            if let Ok(modified) = fm.modified() {
              let now = SystemTime::now();
              let elapsed = now.duration_since(modified).unwrap();
              num_files += 1;
              if elapsed.as_secs() > delete_after_seconds {
                if let Ok(_) = fs::remove_file(file_path.clone()) {
                  num_deleted += 1;
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
  let sub_dir = dotenv::var("SPREADSHEET_SUBDIR").unwrap_or(String::from("/tmp"));
  (tmp_dir, sub_dir)
}

#[tokio::main]
async fn main() {
    let cors = CorsLayer::new()
    .allow_origin(Any)
    .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE]);

    let app = Router::new()
        .route("/", post(upload_asset))
        // The default axum body size limit is 2MiB, so we increase it to 1GiB.
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(cors)
        .into_make_service();
    let ip = dotenv::var("LOCAL_ADDRESS").unwrap_or(String::from("0.0.0.0"));
    let port = dotenv::var("PORT").unwrap_or(String::from("3000"));
    let address = format!("{}:{}", ip, port);
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}