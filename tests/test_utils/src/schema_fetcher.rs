use anyhow::{Context, Result};
use async_trait::async_trait;
use dojo_introspect_types::{DojoSchema, DojoSchemaFetcher, DojoTypeDefSerde};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::path::PathBuf;
use torii_utils::read_json_file;

pub struct FakeProvider {
    pub file_path: PathBuf,
}

#[derive(Deserialize, Serialize)]
struct ModelContract {
    schema: Vec<Felt>,
    use_legacy_storage: bool,
}

fn read_model_schema(path: &PathBuf, contract_address: Felt) -> Result<DojoSchema> {
    let contract: ModelContract =
        read_json_file(&path.join(format!("{contract_address:#x}.json"))).unwrap();
    DojoSchema::dojo_deserialize(
        &mut contract.schema.into_iter(),
        contract.use_legacy_storage,
    )
    .context("Failed to deserialize schema")
}

#[async_trait]
impl DojoSchemaFetcher for FakeProvider {
    async fn schema(&self, contract_address: Felt) -> Result<DojoSchema> {
        read_model_schema(&self.file_path, contract_address)
    }
}
