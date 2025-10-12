use std::{fs::create_dir_all, path::PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use introspect_value::{Field, ToPrimitiveString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use torii_core::{Batch, Envelope, Event, Sink};
use torii_types_introspect::{DeclareTableV1, DeleteRecordsV1, UpdateRecordFieldsV1};

const TABLE_DIR: &str = "tables";
const RECORD_DIR: &str = "records";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonSinkConfig {
    pub file_path: PathBuf,
}

trait ToJson {
    fn to_json_path(&self) -> PathBuf;
}

impl ToJson for PathBuf {
    fn to_json_path(&self) -> PathBuf {
        self.with_extension("json")
    }
}

pub struct JsonSink {
    pub path: PathBuf,
    pub label: String,
    pub table_path: PathBuf,
    pub record_path: PathBuf,
}

fn to_json_object(fields: Vec<Field>) -> Map<String, JsonValue> {
    let mut map = Map::new();
    for Field {
        name,
        attrs: _,
        value,
    } in fields
    {
        map.insert(name, value.into());
    }
    map
}

impl JsonSink {
    pub fn new(path: PathBuf, label: String) -> Result<Self> {
        create_dir_all(&path)?;
        let table_path = path.join(TABLE_DIR);
        let record_path = path.join(RECORD_DIR);
        Ok(Self {
            path,
            label,
            table_path,
            record_path,
        })
    }

    pub fn table_path(&self, table_name: &str) -> PathBuf {
        self.table_path.join(table_name).to_json_path()
    }

    pub fn record_path(&self, table_name: &str, record_id: &str) -> PathBuf {
        self.record_path
            .join(table_name)
            .join(record_id)
            .to_json_path()
    }

    pub fn handle_delete_records(&self, envelope: &Envelope) -> Result<()> {
        let event = envelope
            .downcast::<DeleteRecordsV1>()
            .context("Failed to downcast envelope to DeleteRecordsV1")?;
        for id_field in &event.id_fields {
            let name = id_field
                .value
                .to_primitive_string()
                .context("Failed to convert id_field value to string")?;
            let path = self.record_path(&event.table_name, &name);
            if path.exists() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("Failed to delete record file at path: {:?}", path))?;
            } else {
                tracing::warn!("Record file does not exist at path: {:?}", path);
            }
        }
        Ok(())
    }

    pub fn handle_declare_table(&self, envelope: &Envelope) -> Result<()> {
        let event = envelope
            .downcast::<DeclareTableV1>()
            .context("Failed to downcast envelope to DeclareTableV1")?;
        create_dir_all(self.record_path.join(&event.name)).ok();
        let path = self.table_path(&event.name);
        if path.exists() {
        } else {
            let file = std::fs::File::create(&path)
                .with_context(|| format!("Failed to create table file at path: {:?}", path))?;
            serde_json::to_writer_pretty(file, &event).with_context(|| {
                format!("Failed to write table data to file at path: {:?}", path)
            })?;
        }

        Ok(())
    }

    pub fn handle_update_record_fields(&self, envelope: &Envelope) -> Result<()> {
        let event = envelope
            .downcast::<UpdateRecordFieldsV1>()
            .context("Failed to downcast envelope to UpdateRecordFieldsV1")?;
        let path = self.record_path(
            &event.table_name,
            &event
                .id_field
                .value
                .to_primitive_string()
                .context("Failed to convert id_field value to string")?,
        );
        if path.exists() {
        } else {
            let file = std::fs::File::create(&path)
                .with_context(|| format!("Failed to create record file at path: {:?}", path))?;
            let json = to_json_object(event.fields.clone());
            serde_json::to_writer_pretty(file, &json).with_context(|| {
                format!("Failed to write record data to file at path: {:?}", path)
            })?;
        }
        Ok(())
    }

    pub fn handle_envelope(&self, envelope: &Envelope) -> Result<()> {
        let type_id = envelope.type_id;
        if type_id == DeclareTableV1::TYPE_ID {
            self.handle_declare_table(envelope)?;
        } else if type_id == UpdateRecordFieldsV1::TYPE_ID {
            self.handle_update_record_fields(envelope)?;
        } else if type_id == DeleteRecordsV1::TYPE_ID {
            self.handle_delete_records(envelope)?;
        } else {
            tracing::warn!("Unknown event type_id: {}", type_id);
        };
        Ok(())
    }
}

#[async_trait]
impl Sink for JsonSink {
    fn label(&self) -> &str {
        &self.label
    }

    async fn handle_batch(&self, batch: Batch) -> Result<()> {
        batch
            .items
            .iter()
            .map(|envelope| self.handle_envelope(envelope))
            .collect::<Result<()>>()?;
        Ok(())
    }
}

// pub struct LogSinkFactory;

// #[async_trait]
// impl SinkFactory for LogSinkFactory {
//     fn kind(&self) -> &'static str {
//         "log"
//     }

//     async fn create(&self, name: &str, config: Value) -> Result<Arc<dyn Sink>> {
//         let cfg: LogSinkConfig = if config.is_null() {
//             LogSinkConfig::default()
//         } else {
//             serde_json::from_value(config)?
//         };
//         let label = cfg.label.unwrap_or_else(|| name.to_string());
//         Ok(Arc::new(LogSink::new(label)) as Arc<dyn Sink>)
//     }
// }

// pub fn register(registry: &mut SinkRegistry) {
//     registry.register_factory(Arc::new(LogSinkFactory));
// }
