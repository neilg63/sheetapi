
use serde::{Deserialize, Serialize};
use axum_typed_multipart::{FieldData, TryFromMultipart};
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
    }
  }
}