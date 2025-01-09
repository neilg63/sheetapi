use axum::{
    extract::{Json, Path as PathParam, Query},
    http::StatusCode,
    response::IntoResponse,
};
use crate::{db::get_db_instance, files::*, options::*};
use axum_typed_multipart::TypedMultipart;
use serde_json::{json, Value};
use spreadsheet_to_json::{
    process_spreadsheet_immediate, simple_string_patterns::ToSegments, OptionSet, ReadMode,
};
use std::path::{Path, PathBuf};

#[axum::debug_handler]
pub async fn upload_asset(
    TypedMultipart(request): TypedMultipart<UploadAssetRequest>,
) -> impl IntoResponse {
    let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
    let file_name = build_filename(&request.file);
    let file_path = Path::new(tmp_directory.as_str())
        .join(sub_directory.as_str())
        .join(&file_name);
    // Save the file to the temporary directory
    if let Ok(_fn) = ensure_directory_and_construct_path(&tmp_directory, &sub_directory, &file_name)
    {
        save_file(&request.file, &file_path).ok();
    } else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error_response("Failed to create directory"),
        )
            .into_response();
    }
    let core_options = &request.to_core_options();
    match process_asset_common(file_path, &core_options, false).await {
        Ok(response) => response.into_response(),
        Err((status, message)) => (status, json_error_response(&message)).into_response(),
    }
}

#[axum::debug_handler]
pub async fn process_asset(Json(core_options): Json<CoreOptions>) -> impl IntoResponse {
    let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
    let file_name = core_options
        .filename
        .clone()
        .unwrap_or(String::from("empty.ods"));
    let file_path = Path::new(tmp_directory.as_str())
        .join(sub_directory.as_str())
        .join(&file_name);

    match process_asset_common(file_path, &core_options, true).await {
        Ok(response) => response.into_response(),
        Err((status, message)) => (status, json_error_response(&message)).into_response(),
    }
}

pub async fn check_file(PathParam(file_name): PathParam<String>) -> impl IntoResponse {
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

pub async fn get_dataset(PathParam(id): PathParam<String>, Query(params): Query<QueryFilterParams>) -> impl IntoResponse {
    let db = get_db_instance().await;
    let criteria = params.to_criteria();
    let (start, limit) = params.to_pagination();
    let sort_criteria = params.to_sort_criteria();
    let data = db.fetch_dataset(&id, None, criteria, limit, start, sort_criteria).await;
    let response = json!(data);
    (StatusCode::OK, Json(response))
}

pub async fn welcome() -> impl IntoResponse {
    let response = json!({
        "message": "Welcome to the Spreadsheet to JSON API",
        "routes": {
            "upload": {
                "method": "POST",
                "path": "/upload",
                "type": "multipart/form-data",
                "params": {
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
                "path": "/process","type": "application/json",
                "params": {
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
            "dataset": {
                "method": "GET",
                "path": "/dataset/:dataset_id",
                "path_params": {
                  ":dataset_id": "The ID of the dataset to retrieve"
                },
                "query_params": {
                  "f": "Field name (snake_cased)",
                  "v": "Field value",
                  "o": "Comparison operator (eq, ne, gt, gte, lt, lte, in, nin, regex, starts, ends)",
                  "sort": "Sort field",
                  "dir": "Sort direction (asc or desc)",
                  "start": "Start offset for pagination",
                  "limit": "Number of rows per page"
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
    save_rows: bool,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let default_limit: usize = dotenv::var("DEFAULT_LIMIT")
        .unwrap_or(String::from("1000"))
        .parse()
        .unwrap_or(1000);
    let default_preview_limit = dotenv::var("DEFAULT_PREVIEW_LIMIT")
        .unwrap_or(String::from("25"))
        .parse()
        .unwrap_or(25);
    let max_limit: usize = dotenv::var("MAX_LIMIT")
        .unwrap_or(String::from("10000"))
        .parse()
        .unwrap_or(10000);
    let max_preview_limit = dotenv::var("MAX_PREVIEW_LIMIT")
        .unwrap_or(String::from("50"))
        .parse()
        .unwrap_or(200);

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
        // future use
        // let line_mode = core_options.lines.unwrap_or(false);
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
        let import_id_opt = core_options.import_id.clone();
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
                let mut response = result.to_json();
                if save_rows {
                    let db = get_db_instance().await;
                    let core_options_json = core_options.to_json_value();
                    let rows = result
                        .to_vec()
                        .into_iter()
                        .map(|r| json!(r))
                        .collect::<Vec<Value>>();
                    let import_info = db.save_import_with_rows(&core_options_json, &rows, import_id_opt).await;
                    if let Some((dataset_id, import_id, num_rows)) = import_info {
                        response["dataset"] = json!({
                            "id": json!(dataset_id),
                            "import_id": json!(import_id),
                            "rows": num_rows
                        });
                    }
                }
                Ok(Json(response).into_response())
            }
            Err(_) => {
                remove_uploaded_file(&file_path);
                Err((
                    StatusCode::NOT_ACCEPTABLE,
                    "Failed to process file".to_string(),
                ))
            }
        }
        } else {
            Err((StatusCode::BAD_REQUEST, "No file name provided".to_string()))
        }
    }

fn json_error_response(message: &str) -> Json<serde_json::Value> {
    Json(json!({
        "valid": false,
        "message": message,
    }))
}
