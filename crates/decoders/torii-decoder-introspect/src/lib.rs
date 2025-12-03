use crate::fetcher::SchemaFetcherTrait;
use crate::manager::Manager;
use crate::store::StoreTrait;
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

mod fetcher;
mod manager;
mod parser;
mod store;

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
pub struct IntrospectEventReader<Store, F>
where
    Store: StoreTrait + Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    pub filter: DecoderFilter,
    pub manager: Manager<Store>,
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
impl<Store, F> Decoder for IntrospectEventReader<Store, F>
where
    F: SchemaFetcherTrait + Sync + Send + 'static,
    Store: StoreTrait + Send + Sync + 'static,
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
        let mut keys = event.keys.clone().into_iter();
        let mut data = event.data.clone().into_iter();
        let selector = keys.next().expect("event selector is required");
        // Felts are non structural types, so we can't use a match statement directly.
        // TODO: check if using hashmap would be better.
        let result = match selector.to_raw() {
            CreateColumnGroup::SELECTOR_RAW => {
                self.create_column_group(event, &mut keys, &mut data).await
            }
            CreateTable::SELECTOR_RAW => self.create_table(event, &mut keys, &mut data).await,
            CreateTableWithColumns::SELECTOR_RAW => {
                self.create_table_with_columns(event, &mut keys, &mut data)
                    .await
            }
            CreateTableFromClassHash::SELECTOR_RAW => {
                self.create_table_from_class_hash(event, &mut keys, &mut data)
                    .await
            }
            RenameTable::SELECTOR_RAW => self.rename_table(event, &mut keys, &mut data).await,
            DropTable::SELECTOR_RAW => self.drop_table(event, &mut keys, &mut data).await,
            RenamePrimary::SELECTOR_RAW => self.rename_primary(event, &mut keys, &mut data).await,
            RetypePrimary::SELECTOR_RAW => self.retype_primary(event, &mut keys, &mut data).await,
            AddColumn::SELECTOR_RAW => self.add_column(event, &mut keys, &mut data).await,
            AddColumns::SELECTOR_RAW => self.add_columns(event, &mut keys, &mut data).await,
            RenameColumn::SELECTOR_RAW => self.rename_column(event, &mut keys, &mut data).await,
            RenameColumns::SELECTOR_RAW => self.rename_columns(event, &mut keys, &mut data).await,
            RetypeColumn::SELECTOR_RAW => self.retype_column(event, &mut keys, &mut data).await,
            RetypeColumns::SELECTOR_RAW => self.retype_columns(event, &mut keys, &mut data).await,
            DropColumn::SELECTOR_RAW => self.drop_column(event, &mut keys, &mut data).await,
            DropColumns::SELECTOR_RAW => self.drop_columns(event, &mut keys, &mut data).await,
            InsertRecord::SELECTOR_RAW => self.insert_record(event, &mut keys, &mut data),
            InsertRecords::SELECTOR_RAW => self.insert_records(event, &mut keys, &mut data),
            InsertField::SELECTOR_RAW => self.insert_field(event, &mut keys, &mut data),
            InsertFields::SELECTOR_RAW => self.insert_fields(event, &mut keys, &mut data),
            InsertsField::SELECTOR_RAW => self.inserts_field(event, &mut keys, &mut data),
            InsertsFields::SELECTOR_RAW => self.inserts_fields(event, &mut keys, &mut data),
            InsertFieldGroup::SELECTOR_RAW => self.insert_field_group(event, &mut keys, &mut data),
            InsertFieldGroups::SELECTOR_RAW => {
                self.insert_field_groups(event, &mut keys, &mut data)
            }
            InsertsFieldGroup::SELECTOR_RAW => {
                self.inserts_field_group(event, &mut keys, &mut data)
            }
            InsertsFieldGroups::SELECTOR_RAW => {
                self.inserts_field_groups(event, &mut keys, &mut data)
            }
            DeleteRecord::SELECTOR_RAW => self.delete_record(event, &mut keys, &mut data),
            DeleteRecords::SELECTOR_RAW => self.delete_records(event, &mut keys, &mut data),
            DeleteField::SELECTOR_RAW => self.delete_field(event, &mut keys, &mut data),
            DeleteFields::SELECTOR_RAW => self.delete_fields(event, &mut keys, &mut data),
            DeletesField::SELECTOR_RAW => self.deletes_field(event, &mut keys, &mut data),
            DeletesFields::SELECTOR_RAW => self.deletes_fields(event, &mut keys, &mut data),
            DeleteFieldGroup::SELECTOR_RAW => self.delete_field_group(event, &mut keys, &mut data),
            DeleteFieldGroups::SELECTOR_RAW => {
                self.delete_field_groups(event, &mut keys, &mut data)
            }
            DeletesFieldGroup::SELECTOR_RAW => {
                self.deletes_field_group(event, &mut keys, &mut data)
            }
            DeletesFieldGroups::SELECTOR_RAW => {
                self.deletes_field_groups(event, &mut keys, &mut data)
            }
            _ => Err(anyhow!("unrecognized event selector: {}", selector))?,
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
