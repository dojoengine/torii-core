use crate::manager::{Manager, StoreTrait};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use introspect_events::database::{
    AddColumn, AddColumns, CreateColumnGroup, CreateTable, CreateTableFromClassHash,
    CreateTableWithColumns, DeleteField, DeleteFieldGroup, DeleteFieldGroups, DeleteFields,
    DeleteRecord, DeleteRecords, DeletesField, DeletesFieldGroup, DeletesFieldGroups,
    DeletesFields, DropColumn, DropColumns, DropTable, InsertField, InsertFieldGroup,
    InsertFieldGroups, InsertFields, InsertRecord, InsertRecords, InsertsField, InsertsFieldGroup,
    InsertsFieldGroups, InsertsFields, RenameColumn, RenameColumns, RenamePrimary, RenameTable,
    RetypeColumn, RetypeColumns, RetypePrimary,
};
use introspect_events::event::EventTrait;
use parser::IntrospectParser;
use starknet::core::types::EmittedEvent;
use torii_core::{Decoder, DecoderFilter, Envelope};

mod manager;
mod parser;

const DECODER_NAME: &str = "introspect";

const DOJO_EVENT_IDS: [u64; 0] = [];

// /// Cairo selectors of the events to be processed by this decoder.

// /// Configuration for the introspect decoder.
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct IntrospectDecoderConfig {
//     pub store_path: PathBuf,
//     pub rpc_url: Url,
// }

/// Implementation of the introspect decoder.
pub struct IntrospectEventReader<S, F>
where
    S: StoreTrait + Send + Sync + 'static,
{
    pub filter: DecoderFilter,
    pub manager: Manager<S>,
    pub fetcher: F,
}

// impl IntrospectDecoder<JsonRpcClient<HttpTransport>> {
//     /// Builds the decoder from a configuration.
//     fn from_config(cfg: IntrospectDecoderConfig, contracts: Vec<ContractBinding>) -> Result<Self> {
//         if contracts.is_empty() {
//             return Err(anyhow!(
//                 "introspect decoder requires at least one contract binding"
//             ));
//         }

//         let mut contract_addresses = HashSet::default();
//         let mut selectors = HashSet::default();
//         for selector in DOJO_CAIRO_EVENT_SELECTORS {
//             selectors.insert(selector);
//         }
//         let mut address_selectors = HashMap::new();
//         for binding in contracts {
//             contract_addresses.insert(binding.address);
//             let entry =
//                 address_selectors
//                     .entry(binding.address)
//                     .or_insert_with(|| ContractFilter {
//                         selectors: HashSet::new(),
//                         deployed_at_block: binding.deployed_at_block,
//                     });
//             entry.selectors.extend(selectors.iter().copied());
//             if let Some(block) = binding.deployed_at_block {
//                 entry.deployed_at_block = match entry.deployed_at_block {
//                     Some(existing) => Some(existing.min(block)),
//                     None => Some(block),
//                 };
//             }
//         }
//         let provider = JsonRpcClient::new(HttpTransport::new(cfg.rpc_url));

//         let store = JsonStore::new(&cfg.store_path);

//         let manager = DojoManager::new(store)?;
//         let filter = DecoderFilter {
//             contract_addresses,
//             selectors,
//             address_selectors,
//         };

//         Ok(Self {
//             filter,
//             manager,
//             fetcher: provider,
//         })
//     }
// }

#[async_trait]
impl<F, M> Decoder for IntrospectEventReader<F, M>
where
    F: Sync + Send + 'static,
    M: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        DECODER_NAME
    }

    fn filter(&self) -> &DecoderFilter {
        &self.filter
    }

    fn matches(&self, ev: &EmittedEvent) -> bool {
        if !self.filter.contract_addresses.is_empty()
            && !self.filter.contract_addresses.contains(&ev.from_address)
        {
            return false;
        }

        ev.keys
            .first()
            .map(|key| self.filter.selectors.contains(key))
            .unwrap_or(false)
    }

    fn type_ids(&self) -> &'static [u64] {
        &DOJO_EVENT_IDS
    }

    async fn decode(&self, event: &EmittedEvent) -> Result<Envelope> {
        let mut keys = event.keys.into_iter();
        let mut data = event.data.into_iter();
        let selector = keys.next().expect("event selector is required");
        // Felts are non structural types, so we can't use a match statement directly.
        // TODO: check if using hashmap would be better.
        let result = match selector {
            CreateColumnGroup::SELECTOR => self.create_column_group(event, &mut keys, &mut data),
            CreateTable::SELECTOR => self.create_table(event, &mut keys, &mut data),
            CreateTableWithColumns::SELECTOR => {
                self.create_table_with_columns(event, &mut keys, &mut data)
            }
            CreateTableFromClassHash::SELECTOR => {
                self.create_table_from_class_hash(event, &mut keys, &mut data)
            }
            RenameTable::SELECTOR => self.rename_table(event, &mut keys, &mut data),
            DropTable::SELECTOR => self.drop_table(event, &mut keys, &mut data),
            RenamePrimary::SELECTOR => self.rename_primary(event, &mut keys, &mut data),
            RetypePrimary::SELECTOR => self.retype_primary(event, &mut keys, &mut data),
            AddColumn::SELECTOR => self.add_column(event, &mut keys, &mut data),
            AddColumns::SELECTOR => self.add_columns(event, &mut keys, &mut data),
            RenameColumn::SELECTOR => self.rename_column(event, &mut keys, &mut data),
            RenameColumns::SELECTOR => self.rename_columns(event, &mut keys, &mut data),
            RetypeColumn::SELECTOR => self.retype_column(event, &mut keys, &mut data),
            RetypeColumns::SELECTOR => self.retype_columns(event, &mut keys, &mut data),
            DropColumn::SELECTOR => self.drop_column(event, &mut keys, &mut data),
            DropColumns::SELECTOR => self.drop_columns(event, &mut keys, &mut data),
            InsertRecord::SELECTOR => self.insert_record(event, &mut keys, &mut data),
            InsertRecords::SELECTOR => self.insert_records(event, &mut keys, &mut data),
            InsertField::SELECTOR => self.insert_field(event, &mut keys, &mut data),
            InsertFields::SELECTOR => self.insert_fields(event, &mut keys, &mut data),
            InsertsField::SELECTOR => self.inserts_field(event, &mut keys, &mut data),
            InsertsFields::SELECTOR => self.inserts_fields(event, &mut keys, &mut data),
            InsertFieldGroup::SELECTOR => self.insert_field_group(event, &mut keys, &mut data),
            InsertFieldGroups::SELECTOR => self.insert_field_groups(event, &mut keys, &mut data),
            InsertsFieldGroup::SELECTOR => self.inserts_field_group(event, &mut keys, &mut data),
            InsertsFieldGroups::SELECTOR => self.inserts_field_groups(event, &mut keys, &mut data),
            DeleteRecord::SELECTOR => self.delete_record(event, &mut keys, &mut data),
            DeleteRecords::SELECTOR => self.delete_records(event, &mut keys, &mut data),
            DeleteField::SELECTOR => self.delete_field(event, &mut keys, &mut data),
            DeleteFields::SELECTOR => self.delete_fields(event, &mut keys, &mut data),
            DeletesField::SELECTOR => self.deletes_field(event, &mut keys, &mut data),
            DeletesFields::SELECTOR => self.deletes_fields(event, &mut keys, &mut data),
            DeleteFieldGroup::SELECTOR => self.delete_field_group(event, &mut keys, &mut data),
            DeleteFieldGroups::SELECTOR => self.delete_field_groups(event, &mut keys, &mut data),
            DeletesFieldGroup::SELECTOR => self.deletes_field_group(event, &mut keys, &mut data),
            DeletesFieldGroups::SELECTOR => self.deletes_field_groups(event, &mut keys, &mut data),
        };
        match result {
            Ok(envelope) => Ok(envelope),
            Err(e) => Err(anyhow!(
                "introspect decoder failed to decode event {event:?}\n==> {e}"
            )),
        }
    }
}

// pub struct IntrospectDecoderFactory;

// #[async_trait]
// impl DecoderFactory for IntrospectDecoderFactory {
//     fn kind(&self) -> &'static str {
//         DECODER_NAME
//     }

//     async fn create(
//         &self,
//         config: serde_json::Value,
//         contracts: Vec<ContractBinding>,
//     ) -> Result<Arc<dyn Decoder>> {
//         let cfg: IntrospectDecoderConfig = serde_json::from_value(config)?;
//         let decoder = IntrospectDecoder::from_config(cfg, contracts)?;
//         Ok(Arc::new(decoder))
//     }
// }
