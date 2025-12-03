use starknet_types_core::felt::Felt;
use std::path::PathBuf;

use super::StoreTrait;

fn felt_to_fixed_hex_string(felt: &Felt) -> String {
    format!("0x{:0>32x}", felt)
}
fn felt_to_json_file_name(felt: &Felt) -> String {
    format!("{}.json", felt_to_fixed_hex_string(felt))
}

fn json_file_name_to_felt(file_name: &str) -> Option<Felt> {
    let hex_str = file_name.strip_suffix(".json")?;
    Felt::from_hex(hex_str).ok()
}

pub struct JsonStore {
    pub path: PathBuf,
}

impl JsonStore {
    pub fn new(path: &PathBuf, clean_on_start: bool) -> Self {
        if clean_on_start {
            std::fs::remove_dir_all(path).expect("Unable to clean directory");
        }

        if !path.exists() {
            std::fs::create_dir_all(path).expect("Unable to create directory");
        }
        for p in ["tables", "types", "groups"] {
            let dir_path = path.join(*p);
            if !dir_path.exists() {
                std::fs::create_dir_all(dir_path).expect("Unable to create directory");
            }
        }

        Self {
            path: path.to_path_buf(),
        }
    }
}

impl StoreTrait for JsonStore {}
