use async_trait::async_trait;
use dojo_introspect::{DojoIntrospectResult, DojoSchema, DojoSchemaFetcher, DojoSerde};
use introspect_types::CairoDeserialize;
use resolve_path::PathResolveExt;
use starknet_types_core::felt::Felt;
use std::path::PathBuf;

use crate::read_json_file;

pub struct FakeProvider {
    pub path: PathBuf,
}

impl FakeProvider {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into().resolve().into_owned(),
        }
    }
}

#[derive(serde::Deserialize)]
struct ModelContract {
    schema: Vec<Felt>,
    use_legacy_storage: bool,
}

#[async_trait]
impl DojoSchemaFetcher for FakeProvider {
    async fn schema(&self, contract_address: Felt) -> DojoIntrospectResult<DojoSchema> {
        let ModelContract {
            schema,
            use_legacy_storage: legacy,
        } = read_json_file(&self.path.join(format!("{contract_address:#x}.json"))).unwrap();
        let mut deserializer = DojoSerde::new_from_source(schema, legacy);
        DojoSchema::deserialize(&mut deserializer).map_err(Into::into)
    }
}
