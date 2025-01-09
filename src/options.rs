
use std::str::FromStr;
use bson::{doc, oid::ObjectId, Document};
use fuzzy_datetime::{is_datetime_like, iso_fuzzy_string_to_datetime};
use serde_with::chrono::{self, TimeZone};
use serde::{Deserialize, Serialize};
use axum_typed_multipart::{FieldData, TryFromMultipart};
use spreadsheet_to_json::{is_truthy::is_truthy_core, simple_string_patterns::{IsNumeric, StripCharacters, ToSegments}};
use tempfile::NamedTempFile;

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

#[derive(Serialize, Deserialize)]
pub struct CoreOptions {
  pub filename: Option<String>,
  pub mode: Option<String>,
  pub max: Option<usize>,
  pub keys: Option<String>,
  pub lines: Option<u8>,
  pub cols: Option<String>,
  pub sheet_index: Option<usize>,
  pub header_index: Option<usize>,
  pub dataset_id: Option<String>,
  pub import_id: Option<String>,
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
  pub fn to_json_value(&self) -> serde_json::Value {
    let mode_str = self.mode.clone().unwrap_or("sync".to_string());
    let mut value = serde_json::json!({
      "filename": self.filename.clone().unwrap_or_default(),
      "sheet_index": self.sheet_index.unwrap_or(0),
      "header_index": self.header_index.unwrap_or(0),
      "mode": mode_str,
      "lines": self.lines.unwrap_or(0) > 0,
    });
    if let Some(keys) = self.keys.clone() {
      if keys.len() > 0 {
        value["keys"] = serde_json::json!(keys.to_parts(","));
      }
    }
    if let Some(cols) = self.cols.clone() {
      if cols.len() > 0 {
        value["columns"] = serde_json::json!(cols.to_parts(","));
      }
    }
    value
  }
}

impl UploadAssetRequest {
  pub fn to_core_options(&self) -> CoreOptions {
    CoreOptions {
      filename: self.file.metadata.file_name.clone(),
      mode: self.mode.clone(),
      max: self.max,
      keys: self.keys.clone(),
      lines: self.lines.map(|l| l as u8),
      cols: self.cols.clone(),
      sheet_index: self.sheet_index,
      header_index: self.header_index,
      dataset_id: None,
      import_id: None,
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
                    "like" | "l" => doc! { "$regex": format!("^\\s*{}\\s*$",value.trim()), "$options": "i" },
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

    pub fn to_sort_criteria(&self) -> Option<Document> {
        if let Some(sort) = self.sort.clone() {
            let dir_key = self.dir.clone().unwrap_or("asc".to_string()).trim().to_lowercase();
            let dir = match dir_key.as_str() {
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
            };
            Some(doc! { format!("data.{}", sort): dir })
        } else {
            None
        }
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

