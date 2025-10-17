use std::{
    collections::VecDeque,
    fs::{self, canonicalize},
    path::PathBuf,
};

use serde::Deserialize;
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use torii_utils::read_json_file;

#[derive(Deserialize)]
pub struct Event {
    block_hash: Felt,
    block_number: u64,
    data: Vec<Felt>,
    from_address: Felt,
    keys: Vec<Felt>,
    transaction_hash: Felt,
}

impl Into<EmittedEvent> for Event {
    fn into(self) -> EmittedEvent {
        EmittedEvent {
            block_hash: Some(self.block_hash),
            block_number: Some(self.block_number),
            data: self.data,
            from_address: self.from_address,
            keys: self.keys,
            transaction_hash: self.transaction_hash,
        }
    }
}

#[derive(Deserialize)]
pub struct EventBatch {
    #[allow(dead_code)]
    pub continuation_token: Option<String>,
    pub events: VecDeque<Event>,
}

pub struct EventIterator {
    pub events: VecDeque<Event>,
    pub files: VecDeque<PathBuf>,
    #[allow(dead_code)]
    pub batch: usize,
    #[allow(dead_code)]
    pub event: usize,
}

impl EventIterator {
    pub fn new(path: PathBuf) -> Self {
        let path = canonicalize(path).unwrap();
        let mut paths = fs::read_dir(&path)
            .unwrap()
            .map(|p| p.unwrap().path())
            .collect::<Vec<_>>();
        alphanumeric_sort::sort_path_slice(&mut paths);

        Self {
            events: VecDeque::new(),
            files: paths.into(),
            batch: 0,
            event: 0,
        }
    }
}

impl Iterator for EventIterator {
    type Item = EmittedEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.event += 1;
        match self.events.pop_front() {
            Some(event) => Some(event.into()),
            None => {
                self.events = read_json_file::<EventBatch>(&self.files.pop_front()?)
                    .unwrap()
                    .events;
                self.batch += 1;
                self.event = 0;
                Some(self.events.pop_front()?.into())
            }
        }
    }
}
