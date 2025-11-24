use anyhow::{Context, Result};
use async_trait::async_trait;
use introspect_types::Field;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::fs;
use std::fs::create_dir_all;
use std::path::PathBuf;
use torii_core::{Batch, Envelope, Event, Sink};
use torii_types_introspect::{CreateTableV1, DeleteRecordsV1, InsertFieldsV1};

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

pub fn read_json_file<T>(path: &PathBuf) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let data = fs::read_to_string(path)
        .with_context(|| format!("Failed to read file at path: {:?}", path))?;
    let value = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse JSON from file at path: {:?}", path))?;
    Ok(value)
}

pub fn write_json_file<T>(path: &PathBuf, value: &T) -> Result<()>
where
    T: Serialize,
{
    let file = fs::File::create(path)
        .with_context(|| format!("Failed to create file at path: {:?}", path))?;
    serde_json::to_writer_pretty(file, value)
        .with_context(|| format!("Failed to write JSON to file at path: {:?}", path))?;
    Ok(())
}

pub struct JsonSink {
    pub path: PathBuf,
    pub label: String,
    pub table_path: PathBuf,
    pub record_path: PathBuf,
}

fn fields_to_json_array(fields: Vec<Field>) -> Vec<(String, JsonValue)> {
    fields
        .into_iter()
        .map(|field| (field.name, field.value.into()))
        .collect()
}

fn to_json_object(fields: Vec<Field>) -> Map<String, JsonValue> {
    let mut map = Map::new();
    for Field {
        id: _,
        name,
        attributes: _,
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
        create_dir_all(&table_path)?;
        create_dir_all(&record_path)?;
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
        for value in &event.records {
            let name = value.to_string();
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
            .downcast::<CreateTableV1>()
            .context("Failed to downcast envelope to DeclareTableV1")?;
        create_dir_all(self.record_path.join(&event.name)).ok();
        let path = self.table_path(&event.name);
        if path.exists() {
        } else {
            write_json_file(&path, &event)?;
        }
        Ok(())
    }

    pub fn handle_update_record_fields(&self, envelope: &Envelope) -> Result<()> {
        let event = envelope
            .downcast::<UpdateRecordFieldsV1>()
            .context("Failed to downcast envelope to UpdateRecordFieldsV1")?;
        let path = self.record_path(&event.table_name, &event.primary.value.to_string());

        let data = if path.exists() {
            let mut data: Map<String, JsonValue> = read_json_file(&path)?;
            for (key, value) in fields_to_json_array(event.fields.clone()) {
                data.insert(key, value);
            }
            data
        } else {
            to_json_object(event.fields.clone())
        };
        write_json_file(&path, &data)?;
        Ok(())
    }

    pub fn handle_envelope(&self, envelope: &Envelope) -> Result<()> {
        let type_id = envelope.type_id;
        if type_id == CreateTableV1::TYPE_ID {
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
