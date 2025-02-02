use axum::{
    extract::{Json, Multipart, Path as PathParam, Query},
    http::StatusCode,
    response::IntoResponse,
};
use crate::{db::get_db_instance, files::*, options::*};
use serde_json::{json, Value};
use spreadsheet_to_json::{
    process_spreadsheet_immediate, simple_string_patterns::ToSegments, OptionSet, ReadMode,
};
use std::path::{Path, PathBuf};

#[axum::debug_handler]
pub async fn upload_asset(multipart: Multipart) -> impl IntoResponse {
    let request_result = UploadAssetRequest::from_multipart(multipart).await;
    match request_result {
        Ok(request) => {
            let (tmp_directory, sub_directory) = get_tmp_and_sub_directories();
            let file_name = build_filename(&request.file);
            let file_path = Path::new(tmp_directory.as_str())
                .join(sub_directory.as_str())
                .join(&file_name);
            let core_options = &request.to_core_options();
            // Save the file to the temporary directory
            if let Ok(_fn) = ensure_directory_and_construct_path(&tmp_directory, &sub_directory, &file_name)
            {
                save_file(&request.file, &file_path).ok();
            } else {
                return (StatusCode::NOT_FOUND, json_error_response("Failed to access or create directory.")).into_response();
            }
            if core_options.filename.is_none() {
                return (StatusCode::BAD_REQUEST, json_error_response("No filename provided")).into_response();
            } 
            match process_asset_common(file_path, &core_options, false).await {
                Ok(response) => response.into_response(),
                Err((status, message)) => (status, message).into_response(),
            }
        }
        Err(error) => (StatusCode::BAD_REQUEST, json_error_response(&error.to_string())).into_response(),
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
        Err((status, message)) => (status, message).into_response(),
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
    let data_opt = db.fetch_dataset(&id, None, criteria, limit, start, sort_criteria).await;
    if let Some(data) = data_opt {
        (StatusCode::OK, Json(json!(data)))
    } else {
        (StatusCode::NOT_FOUND, json_error_response("The requested dataset was not found."))
    }
}

pub async fn list_datasets(Query(params): Query<QueryFilterParams>) -> impl IntoResponse {
    let db = get_db_instance().await;
    let criteria = params.to_search_criteria();
    let sort_criteria = params.to_list_sort_criteria();
    let (start, limit) = params.to_pagination();
    let (total, rows) = db.get_datasets(criteria, limit, start, sort_criteria).await;
    let response = json!({
        "total": total.unwrap_or(0),
        "start": start,
        "limit": limit,
        "rows": rows
    });
    (StatusCode::OK, Json(response))
}

pub async fn welcome() -> impl IntoResponse {
    let response = json!({
        "title": "Spreadsheet to JSON API",
        "description": "Upload spreadsheets (Excel: xlsx, xlsb, xls, LibreOffice: odt, CSV and TSV), convert them to JSON and create a Web endpoint",
        "version": "0.1.0",
        "max_upate_size": get_max_upload_size(),
        "max_body_size": get_max_body_size(),
        "max_output_rows": get_max_output_rows(),
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
            "datasets": {
                "method": "GET",
                "path": "/datasets",
                "query_params": {
                  "q": "Search query within file name, titles, or descriptions",
                  "u": "user reference or ID",
                  "sort": "Sort field (created or updated), default by newest first",
                  "dir": "Sort direction (asc or desc), default desc for created",
                  "start": "Start offset for pagination",
                  "limit": "Number of rows per page"
                },
                "description": "List imported datasets by user"
            },
            "check-file": {
                "method": "GET",
                "path": "/check-file/:file_name",
                "description": "Check if a file exists in the temporary directory after initial upload"
            }
        }
    });
    (StatusCode::OK, Json(response))
}


pub async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, json_error_response("The requested resource was not found."))
}

async fn process_asset_common(
    file_path: PathBuf,
    core_options: &CoreOptions,
    save_rows: bool,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
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
        .unwrap_or(String::from("200"))
        .parse()
        .unwrap_or(200);

    let mut col_values: Vec<serde_json::Value> = vec![];
    if let Some(file_name) = core_options.filename.clone() {
        let mode_key = core_options.mode.clone().unwrap_or("sync".to_string());
        let read_mode = ReadMode::from_key(&mode_key);
        let is_preview = read_mode.is_multimode();
        let append = core_options.append.unwrap_or(false);
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
                let file_name_clone = file_name.clone();
                tokio::spawn(async move {
                    let file_name = file_name_clone;
                    // Clean-up logic here
                    if let Ok((num_deleted, num_files)) = perform_cleanup(Some(&file_name)).await {
                        println!("Deleted {} of {} files", num_deleted, num_files);
                    }
                });
                
                if save_rows {
                    let mut response = result.to_json();
                    let db = get_db_instance().await;
                    let core_options_json = core_options.to_json_value();
                    let rows = result
                        .to_vec()
                        .into_iter()
                        .map(|r| json!(r))
                        .collect::<Vec<Value>>();
                    let import_info = db.save_import_with_rows(&core_options_json, &rows, import_id_opt, append).await;
                    
                    if let Some((dataset_id, import_id, num_rows)) = import_info {
                        let max_output_rows = get_max_output_rows();
                        let (limit_rows, num_showing) = if num_rows > max_output_rows {
                            (true, max_output_rows)
                        } else {
                            (false, num_rows)
                        };
                        if limit_rows {
                            response["data"] = json!(rows[..max_output_rows]);
                        } else {
                            response["data"] = json!(rows);
                        }
                        response["dataset"] = json!({
                            "id": json!(dataset_id),
                            "import_id": json!(import_id),
                            "rows": num_rows,
                            "showing": num_showing
                        });
                    }
                    Ok(Json(response).into_response()) 
                } else {
                    Ok(Json(result.to_json()).into_response())
                }
                // Return success response
            }
            Err(_) => {
                remove_uploaded_file(&file_path);
                Err((
                    StatusCode::NOT_ACCEPTABLE,
                    json_error_response("Failed to process file")
                )) // Return error response
            }
        }
    } else {
        Err((StatusCode::BAD_REQUEST, json_error_response("No file name provided"))) // Return error response
    }
}

fn json_error_response(message: &str) -> Json<serde_json::Value> {
    Json(json!({
        "valid": false,
        "message": message,
    }))
}