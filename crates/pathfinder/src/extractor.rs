use crate::{connect, EventFetcher, PFResult};
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use rusqlite::Connection;
use starknet::core::types::EmittedEvent;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use torii::etl::{BlockContext, EngineDb, ExtractionBatch, Extractor};

#[derive(Debug)]
pub struct PathfinderExtractor {
    pub conn: Mutex<Connection>,
    pub batch: u64,
    pub current: u64,
    pub end: u64,
}

impl PathfinderExtractor {
    pub fn new<P: AsRef<Path>>(path: P, batch: u64, start: u64, end: u64) -> PFResult<Self> {
        Ok(Self {
            conn: connect(path)?.into(),
            batch,
            current: start,
            end,
        })
    }

    pub fn next_batch(&mut self) -> PFResult<(Vec<BlockContext>, Vec<EmittedEvent>)> {
        let next = (self.current + self.batch).min(self.end);
        let (blocks, events) = self
            .conn
            .lock()?
            .get_emitted_events_with_context(self.current, next - 1)?;
        self.current = next;
        Ok((blocks, events.into_iter().map(Into::into).collect()))
    }
}

#[async_trait]
impl Extractor for PathfinderExtractor {
    async fn extract(
        &mut self,
        _cursor: Option<String>,
        _engine_db: &EngineDb,
    ) -> AnyResult<ExtractionBatch> {
        let (blocks, events) = self.next_batch()?;
        let blocks = blocks
            .into_iter()
            .map(|b| (b.number, Arc::new(b)))
            .collect();
        Ok(ExtractionBatch {
            events,
            blocks,
            transactions: HashMap::new(),
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: None,
            chain_head: None,
        })
    }
    fn is_finished(&self) -> bool {
        self.current >= self.end
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
