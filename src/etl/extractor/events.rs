use super::FeltMap;
use starknet::core::types::{
    BlockWithReceipts, DeclareTransactionReceipt, DeployAccountTransactionReceipt,
    DeployTransactionReceipt, EmittedEvent, Event, Felt, InvokeTransactionReceipt,
    L1HandlerTransactionReceipt, TransactionReceipt, TransactionWithReceipt,
};
use std::mem;

#[derive(Debug, Clone, Default)]
pub enum ExtractedEvents {
    Contract(FeltMap<ContractEvents>),
    Block(Vec<BlockEvents>),
    #[default]
    None,
}

#[derive(Debug, Clone, Default)]
pub struct ContractEvents {
    pub contract_address: Felt,
    pub transactions: Vec<TransactionEvents>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionEvents {
    pub block_number: u64,
    pub transaction_hash: Felt,
    pub events: Vec<EventData>,
}

#[derive(Debug, Clone)]
pub struct BlockEvents {
    pub block: BlockWithReceipts,
    pub events: Vec<BlockTransactionEvents>,
}

#[derive(Debug, Clone, Default)]
pub struct BlockTransactionEvents {
    pub transaction_hash: Felt,
    pub contract_events: FeltMap<Vec<EventData>>,
}

#[derive(Debug, Clone, Default)]
pub struct EventData {
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
}

impl From<EmittedEvent> for TransactionEvents {
    fn from(value: EmittedEvent) -> Self {
        TransactionEvents {
            block_number: value.block_number.unwrap_or_default(),
            transaction_hash: value.transaction_hash,
            events: vec![EventData {
                keys: value.keys,
                data: value.data,
            }],
        }
    }
}

impl From<EmittedEvent> for EventData {
    fn from(value: EmittedEvent) -> Self {
        EventData {
            keys: value.keys,
            data: value.data,
        }
    }
}

impl From<BlockWithReceipts> for BlockEvents {
    fn from(mut block: BlockWithReceipts) -> Self {
        let mut events = Vec::with_capacity(block.transactions.len());
        block.extract_block_events(&mut events);
        BlockEvents { block, events }
    }
}

pub trait ExtractBlockTransactionEvents {
    fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>);
}

impl ExtractBlockTransactionEvents for BlockWithReceipts {
    fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
        for tx in self.transactions.iter_mut() {
            tx.extract_block_events(contract_txs);
        }
    }
}

impl ExtractBlockTransactionEvents for TransactionWithReceipt {
    fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
        self.receipt.extract_block_events(contract_txs);
    }
}

impl ExtractBlockTransactionEvents for TransactionReceipt {
    fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
        contract_txs.push(BlockTransactionEvents {
            transaction_hash: *self.transaction_hash(),
            contract_events: self.take_event_datas(),
        })
    }
}

pub trait ExtractEventData<'a> {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>);
    fn take_event_datas(&'a mut self) -> FeltMap<Vec<EventData>> {
        let mut events = FeltMap::default();
        self.extract_event_data(&mut events);
        events
    }
}

impl<'a> ExtractEventData<'a> for Vec<Event> {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        for event in self.iter_mut() {
            events
                .entry(event.from_address)
                .or_default()
                .push(EventData {
                    keys: mem::take(&mut event.keys),
                    data: mem::take(&mut event.data),
                });
        }
    }
}

impl<'a> ExtractEventData<'a> for InvokeTransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        self.events.extract_event_data(events);
    }
}

impl<'a> ExtractEventData<'a> for L1HandlerTransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        self.events.extract_event_data(events);
    }
}

impl<'a> ExtractEventData<'a> for DeclareTransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        self.events.extract_event_data(events);
    }
}

impl<'a> ExtractEventData<'a> for DeployTransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        self.events.extract_event_data(events);
    }
}

impl<'a> ExtractEventData<'a> for DeployAccountTransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        self.events.extract_event_data(events);
    }
}

impl<'a> ExtractEventData<'a> for TransactionReceipt {
    fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<EventData>>) {
        match self {
            TransactionReceipt::Invoke(invoke) => invoke.extract_event_data(events),
            TransactionReceipt::L1Handler(l1_handler) => l1_handler.extract_event_data(events),
            TransactionReceipt::Declare(declare) => declare.extract_event_data(events),
            TransactionReceipt::Deploy(deploy) => deploy.extract_event_data(events),
            TransactionReceipt::DeployAccount(deploy_account) => {
                deploy_account.extract_event_data(events)
            }
        }
    }
}
