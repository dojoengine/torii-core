use resolve_path::PathResolveExt;
use serde::Deserialize;
use serde_json::Error;
use std::fs;
use std::path::PathBuf;

pub fn read_json_file<T>(path: &PathBuf) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    println!("Reading JSON file from path: {}", path.display());
    let data = fs::read_to_string(path).map_err(Error::io)?;
    serde_json::from_str(&data)
}

pub fn resolve_path_like<P: Into<PathBuf>>(path: P) -> PathBuf {
    path.into().resolve().into_owned()
}
