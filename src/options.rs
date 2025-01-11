
use std::str::FromStr;
use axum::extract::Multipart;
use bson::{doc, oid::ObjectId, Document};
use fuzzy_datetime::{is_datetime_like, iso_fuzzy_string_to_datetime};
use serde_json::{json, Value};
use serde_with::chrono::{self, TimeZone};
use serde::{Deserialize, Serialize};
use axum_typed_multipart::{FieldData, TryFromField, TryFromMultipart};
use spreadsheet_to_json::{is_truthy::is_truthy_core, simple_string_patterns::{IsNumeric, SimpleMatch, StripCharacters, ToSegments}};
use tempfile::NamedTempFile;

const DEFAULT_MAX_UPLOAD_SIZE: usize = 50 * 1024 * 1024;

pub fn get_max_upload_size() -> usize {
  if let Ok(max_size_val) = dotenv::var("MAX_UPLOAD_SIZE") {
    if let Ok(size_val) = max_size_val.parse::<usize>() {
      return size_val;
    }
  }
  DEFAULT_MAX_UPLOAD_SIZE
}

pub fn get_max_body_size() -> usize {
  get_max_upload_size() * 2 + 32 * 1024
}

#[derive(TryFromMultipart, Debug)]
pub struct UploadAssetRequest {
  pub file: FieldData<NamedTempFile>,
  pub mode: Option<String>,
  pub max: Option<usize>,
  pub keys: Option<String>,
  pub lines: Option<usize>,
  pub cols: Option<String>,
  pub sheet_index: Option<usize>,
  pub header_index: Option<usize>,
}

impl UploadAssetRequest {
  pub async fn from_multipart(mut multipart: Multipart) -> Self {
    let mut file: Option<FieldData<NamedTempFile>> = None;
    let mut mode: Option<String> = None;
    let mut max: Option<usize> = None;
    let mut keys: Option<String> = None;
    let mut lines: Option<usize> = None;
    let mut cols: Option<String> = None;
    let mut sheet_index: Option<usize> = None;
    let mut header_index: Option<usize> = None;

    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        match name.as_str() {
            "file" => {
              let temp_file = NamedTempFile::new().unwrap();
              let max_size = get_max_upload_size();
              let field_data = FieldData::try_from_field(field, Some(max_size)).await.unwrap(); // Set max size to 10 MiB
              file = Some(field_data);
            }
            "mode" => {
                mode = Some(field.text().await.unwrap());
            }
            "max" => {
                max = Some(field.text().await.unwrap().parse().unwrap());
            }
            "keys" => {
                keys = Some(field.text().await.unwrap());
            }
            "lines" => {
                lines = Some(field.text().await.unwrap().parse().unwrap());
            }
            "cols" => {
                cols = Some(field.text().await.unwrap());
            }
            "sheet_index" => {
                sheet_index = Some(field.text().await.unwrap().parse().unwrap());
            }
            "header_index" => {
                header_index = Some(field.text().await.unwrap().parse().unwrap());
            }
            _ => {}
        }
    }

    UploadAssetRequest {
        file: file.unwrap(),
        mode,
        max,
        keys,
        lines,
        cols,
        sheet_index,
        header_index,
    }
  }

}

#[derive(Serialize, Deserialize)]
pub struct CoreOptions {
  pub filename: Option<String>,
  pub title: Option<String>,
  pub description: Option<String>,
  pub user_ref: Option<String>,
  // process mode. Currently we're mainly using preview after the initial upload and then sync with a sheet_index
  // to save data. In future, we'll use async to process large files in the background.
  pub mode: Option<String>,
  pub max: Option<usize>,
  // comma separated list of key names
  pub keys: Option<String>,
  // comma separated list of column names with target data types after colons
  // eg. id,name,height:float,width:float,
  pub cols: Option<String>,
  pub sheet_index: Option<usize>,
  pub header_index: Option<usize>,
  // refer directly to a dataset, assumed to have the same schema
  pub dataset_id: Option<String>,
  // update within a specific import retaining the same id
  pub import_id: Option<String>,
  // append or replace data with the same dataset_id and/or import_id
  pub append: Option<bool>,
  // JSON lines reserved for future use when exporting data to large files
  pub lines: Option<bool>,
}

fn listing_limit() -> u64 {
  if let Ok(listing_limit_val) = dotenv::var("LISTING_LIMIT") {
    if let Ok(limit_val) = listing_limit_val.parse::<u64>() {
      return limit_val;
    }
  }
  10_000
}

impl CoreOptions {
  pub fn to_json_value(&self) -> Value {
    let mode_str = self.mode.clone().unwrap_or("sync".to_string());
    let mut value = json!({
      "filename": self.filename.clone().unwrap_or_default(),
      "title": self.title.clone().unwrap_or_default(),
      "description": self.description.clone().unwrap_or_default(),
      "user_ref": self.user_ref.clone().unwrap_or_default(),
      "sheet_index": self.sheet_index.unwrap_or(0),
      "header_index": self.header_index.unwrap_or(0),
      "mode": mode_str,
      "append": self.append.unwrap_or(false),
      "lines": self.lines
    });
    if let Some(keys) = self.keys.clone() {
      if keys.len() > 0 {
        value["keys"] = json!(keys.to_parts(","));
      }
    }
    if let Some(cols) = self.cols.clone() {
      if cols.len() > 0 {
        value["columns"] = json!(cols.to_parts(","));
      }
    }
    if let Some(d_id) = self.dataset_id.clone() {
      value["dataset_id"] = json!(d_id);
    }
    if let Some(i_id) = self.import_id.clone() {
      value["import_id"] = json!(i_id);
    }
    value
  }

  pub fn append_mode(&self) -> bool {
    self.append.unwrap_or(false)
  }
}

impl UploadAssetRequest {
  pub fn to_core_options(&self) -> CoreOptions {
    CoreOptions {
      filename: self.file.metadata.file_name.clone(),
      title: None,
      description: None,
      user_ref: None,
      mode: self.mode.clone(),
      max: self.max,
      keys: self.keys.clone(),
      lines: self.lines.map(|l| l > 0),
      cols: self.cols.clone(),
      sheet_index: self.sheet_index,
      header_index: self.header_index,
      dataset_id: None,
      import_id: None,
      append: None,
    }
  }
}

pub enum DataSetMatcher {
  NameIndex(String, u32),
  Id(String),
}

impl DataSetMatcher {
  pub fn from_name_index(name: &str, index: u32) -> Self {
    DataSetMatcher::NameIndex(name.to_string(), index)
  }

  pub fn from_id(id: &str) -> Self {
    DataSetMatcher::Id(id.to_string())
  }

  pub fn to_criteria(&self) -> Document {
    match self {
      DataSetMatcher::NameIndex(name, index) => {
        doc! {
          "name": name,
          "sheet_index": index,
        }
      },
      DataSetMatcher::Id(id) => {
        doc! {
          "_id": ObjectId::from_str(id).ok(),
        }
      }

    }

  }
}


#[derive(Deserialize)]
pub struct QueryFilterParams {
    pub f: Option<String>,
    pub v: Option<String>,
    pub o: Option<String>,
    pub dt: Option<String>,
    pub sort: Option<String>,
    pub dir: Option<String>,
    pub import: Option<String>,
    pub start: Option<u64>,
    pub limit: Option<u64>,
    pub q: Option<String>,
    pub u: Option<String>, // user reference
}

impl QueryFilterParams {
    pub fn to_criteria(&self) -> Option<Document> {
        let mut criteria = doc! {};
        if let Some(field) = self.f.clone() {
            if let Some(value) = self.v.clone() {
              let dt_key = self.dt.clone().unwrap_or("string".to_string());
              let data_type = CastDataType::from_str(dt_key.as_str());
                let operator = self.o.clone().unwrap_or("eq".to_string()).to_lowercase().strip_non_alphanum();
                let cv = match operator.as_str() {
                    "ne" => cast_to_comparison("$ne", &value, &data_type),
                    "gt" => cast_to_comparison("$gt", &value, &data_type),
                    "gte" => cast_to_comparison("$gte", &value, &data_type),
                    "lt" => cast_to_comparison("$lt", &value, &data_type),
                    "lte" => cast_to_comparison("$lte", &value, &data_type),
                    "in" => doc! { "$in": value.to_parts(",") },
                    "nin" => doc! { "$nin": value.to_parts(",") },
                    "r" | "regex" | "regexp" | "rgx" => doc! { "$regex": value, "$options": "i" },
                    "rcs" | "rc" | "regexc" | "regexpc" | "rgxc" => doc! { "$regex": value },
                    "like" | "l" => doc! { "$regex": str_to_like_pattern(&value), "$options": "i" },
                    "starts" | "startswith" => doc! { "$regex": format!("^{}",value.trim()), "$options": "i" },
                    "ends" | "endswith" => doc! { "$regex": format!("{}$",value.trim()), "$options": "i" },
                    _ => cast_to_comparison("$eq", &value, &data_type),
                };
                criteria = doc! { format!("data.{}", field): cv };
            }
        }
        if criteria.is_empty() {
            None
        } else {
            Some(criteria)
        }
    }

    pub fn to_search_criteria(&self) -> Option<Document> {
      let mut criteria = doc! {};
      if let Some(q) = self.q.clone() {
        let s_str = format!("\\b\\s*{}", q.trim());
          criteria = doc! { "$or": [
              { "name": { "$regex": &s_str, "$options": "i" } },
              { "imports.filename": { "$regex": &s_str, "$options": "i" } },
              { "title": { "$regex": &s_str, "$options": "i" } },
              { "description": { "$regex": &s_str, "$options": "i" } },
          ] };
      }
      if let Some(u) = self.u.clone() {
        let u_str = format!("^{}\\b", u.trim());
          criteria = doc! { "user_ref": { "$regex": &u_str, "$options": "i" } };
      }
      if criteria.is_empty() {
          None
      } else {
          Some(criteria)
      }
  }

    pub fn to_sort_criteria(&self) -> Option<Document> {
        if let Some(sort) = self.sort.clone() {
          let dir_key = self.dir.clone().unwrap_or("asc".to_string());
          let dir = match_sort_direction(&dir_key);
          Some(doc! { format!("data.{}", sort): dir })
        } else {
          None
        }
    }

    pub fn to_list_sort_criteria(&self) -> Option<Document> {
      let sort = self.sort.clone().unwrap_or("created_at".to_string()); 
      let mut sort_field = "created_at";
      if sort.starts_with_ci("updat") || sort.starts_with_ci("modif") || sort.starts_with_ci("old") {
        sort_field = "updated_at";
      }
      let dir_key = self.dir.clone().unwrap_or("desc".to_string());
      let dir = match_sort_direction(&dir_key);
      Some(doc! { sort_field.to_string(): dir })
    }

    pub fn to_pagination(&self) -> (u64, u64) {
        let start = self.start.unwrap_or(0);
        let mut limit = self.limit.unwrap_or(100);
        let max = listing_limit();
        if limit > max {
            limit = max;
        }
        (start, limit)
    }
    
}

#[derive(Debug, Clone)]
pub enum CastDataType {
  String,
  Float,
  Integer,
  Date,
  DateTime,
  Boolean,
}

impl CastDataType {
  pub fn from_str(key: &str) -> Self {
    match key.to_lowercase().as_str() {
      "float" | "number" =>  CastDataType::Float,
      "int" | "integer" =>  CastDataType::Integer,
      "date" => CastDataType::Date,
      "datetime" => CastDataType::DateTime,
      "bool" | "boolean" => CastDataType::Boolean,
      _ => CastDataType::String,
    }
  }

  pub fn is_numeric(&self) -> bool {
    match self {
      CastDataType::Float | CastDataType::Integer => true,
      _ => false,
    }
  }

  pub fn is_bool(&self) -> bool {
    match self {
      CastDataType::Boolean => true,
      _ => false,
    }
  }

  pub fn is_datelike(&self) -> bool {
    match self {
      CastDataType::Date | CastDataType::DateTime => true,
      _ => false,
    }
  }

  pub fn is_datetime(&self) -> bool {
    match self {
      CastDataType::DateTime => true,
      _ => false,
    }
  }

  pub fn is_integer(&self) -> bool {
    match self {
      CastDataType::Integer => true,
      _ => false,
    }
  }
}

#[derive(Debug, Clone)]
pub enum ReplaceMode {
  ReplaceAll,
  ReplaceImport,
  Append,
}

impl ReplaceMode {
  pub fn new(append: bool, has_import_id: bool) -> Self {
    if append {
      ReplaceMode::Append
    } else if has_import_id {
      ReplaceMode::ReplaceImport
    } else {
      ReplaceMode::ReplaceAll
    }
  }
}

fn cast_to_comparison(op: &str, value: &str, dt: &CastDataType) -> Document {
  if value.is_numeric() || dt.is_numeric() {
    if dt.is_integer() {
      if let Ok(num_val) = value.parse::<i64>() {
        return doc! { op.to_string(): num_val }
      }
    } else {
      if let Ok(num_val) = value.parse::<f64>() {
        return doc! { op.to_string(): num_val }
      }
    }
  } else if is_datetime_like(value) {
    if let Ok(date_val) = iso_fuzzy_string_to_datetime(value) {
      let dt = chrono::Utc.from_utc_datetime(&date_val);
      return doc! { op.to_string(): dt }
    }
  } else {
    if let Some(is_true) = is_truthy_core(value, false) {
      return doc! { op.to_string(): is_true }
    } else {
      return doc! { op.to_string(): value.to_string() }
    }
  }
  return doc! { op.to_string(): value.to_string() }
}

fn match_sort_direction(key: &str) -> i32 {
  let dir_key = key.trim().to_lowercase();
  match dir_key.as_str() {
      "asc" => 1,
      "desc" => -1,
      _ => {
          if dir_key.is_numeric() {
            if let Ok(num_val) = dir_key.parse::<i32>() {
                if num_val < 0 {
                  -1
                } else {
                  1
                }
            } else {
              1
            }
        } else if dir_key.starts_with("desc") || dir_key.starts_with("down") || dir_key.starts_with("dsc") {
          -1
        } else {
          1
        }
      }
  }
}

fn str_to_like_pattern(value: &str) -> String {
  format!("^\\s*{}\\s*$", value.replace('.', ".\\.").replace('?', ".\\?").replace('%', ".*?").trim())
}

