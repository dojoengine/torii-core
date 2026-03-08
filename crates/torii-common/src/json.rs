use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Error as JsonError;
use std::fs::{self, DirEntry};
use std::io::Error as IoError;
use std::path::{Path, PathBuf};

pub trait JsonFs {
    fn load<T: DeserializeOwned>(&self) -> Result<T, JsonError>;
    fn dump<T: Serialize>(&self, data: &T) -> Result<(), JsonError>;
}

impl JsonFs for Path {
    fn load<T: DeserializeOwned>(&self) -> Result<T, JsonError> {
        fs::read_to_string(self)
            .map_err(serde_json::Error::io)
            .and_then(|s| serde_json::from_str(&s))
    }

    fn dump<T: Serialize>(&self, data: &T) -> Result<(), JsonError> {
        std::fs::write(self, serde_json::to_string_pretty(data)?).map_err(serde_json::Error::io)
    }
}

impl JsonFs for PathBuf {
    fn load<T: DeserializeOwned>(&self) -> Result<T, JsonError> {
        Path::load(&self)
    }

    fn dump<T: Serialize>(&self, data: &T) -> Result<(), JsonError> {
        Path::dump(&self, data)
    }
}

impl JsonFs for DirEntry {
    fn load<T: DeserializeOwned>(&self) -> Result<T, JsonError> {
        self.path().load()
    }

    fn dump<T: Serialize>(&self, data: &T) -> Result<(), JsonError> {
        self.path().dump(data)
    }
}

impl JsonFs for Result<DirEntry, IoError> {
    fn load<T: DeserializeOwned>(&self) -> Result<T, JsonError> {
        match self {
            Ok(entry) => entry.load(),
            Err(e) => Err(serde_json::Error::io(e.kind().into())),
        }
    }

    fn dump<T: Serialize>(&self, data: &T) -> Result<(), JsonError> {
        match self {
            Ok(entry) => entry.dump(data),
            Err(e) => Err(serde_json::Error::io(e.kind().into())),
        }
    }
}
