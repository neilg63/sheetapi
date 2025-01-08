
use std::str::FromStr;
use bson::{doc, oid::ObjectId, Document};
use serde::{Deserialize, Serialize};
use axum_typed_multipart::{FieldData, TryFromMultipart};
use spreadsheet_to_json::simple_string_patterns::ToSegments;
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
          "index": index,
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
    pub start: Option<u64>,
    pub limit: Option<u64>,
}

impl QueryFilterParams {
    pub fn to_criteria(&self) -> Option<Document> {
        let mut criteria = doc! {};
        if let Some(field) = self.f.clone() {
            if let Some(value) = self.v.clone() {
                let operator = self.o.clone().unwrap_or("eq".to_string());
                let value = match operator.as_str() {
                    "ne" => doc! { "$ne": value },
                    "gt" => doc! { "$gt": value },
                    "gte" => doc! { "$gte": value },
                    "lt" => doc! { "$lt": value },
                    "lte" => doc! { "$lte": value },
                    "in" => doc! { "$in": value.to_parts(",") },
                    "nin" => doc! { "$nin": value.to_parts(",") },
                    "r" | "regex" => doc! { "$regex": value, "$options": "i" },
                    "starts" | "starts_with" => doc! { "$regex": format!("^{}",value.trim()), "$options": "i" },
                    "ends" | "ends_with" => doc! { "$regex": format!("{}$",value.trim()), "$options": "i" },
                    _ => doc! { "$eq": value },
                };
                criteria = doc! { format!("data.{}", field): value };
            }
        }
        if criteria.is_empty() {
            None
        } else {
            Some(criteria)
        }
    }
    
}