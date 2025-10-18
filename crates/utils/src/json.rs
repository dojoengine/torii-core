use std::{fs, path::PathBuf};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JsonFsErrors {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("JSON Deserialize Error: {0}")]
    JsonDeserializeError(#[from] serde_json::Error),
}

pub fn read_json_file<T>(path: &PathBuf) -> Result<T, JsonFsErrors>
where
    T: for<'de> Deserialize<'de>,
{
    let data = fs::read_to_string(path)?;
    let value = serde_json::from_str(&data)?;
    Ok(value)
}
