use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use starknet::{
    core::types::{BlockId, BlockTag, EmittedEvent, EventFilter, Felt, FunctionCall},
    macros::selector,
    providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider, Url},
};
use tokio::sync::mpsc;

const CHUNK_SIZE: u64 = 1024;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    contract: String,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value = "https://api.cartridge.gg/x/starknet/mainnet")]
    rpc_url: String,
}

#[derive(Serialize)]
pub struct EventBatch {
    pub continuation_token: Option<String>,
    pub events: Vec<EmittedEvent>,
}

#[derive(Serialize)]
struct ContractData {
    schema: Vec<Felt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    use_legacy_storage: Option<bool>,
}

// — Fetcher ——————————————————————————————————————————————————————————————————

struct EventFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
    contract: Felt,
}

impl EventFetcher {
    async fn run(self, tx: mpsc::Sender<EventBatch>) -> Result<()> {
        let mut token: Option<String> = None;
        loop {
            let result = self
                .provider
                .get_events(
                    EventFilter {
                        from_block: None,
                        to_block: None,
                        address: Some(self.contract),
                        keys: None,
                    },
                    token.clone(),
                    CHUNK_SIZE,
                )
                .await?;

            tx.send(EventBatch {
                continuation_token: token.clone(),
                events: result.events,
            })
            .await?;

            token = result.continuation_token;
            if token.is_none() {
                break;
            }
        }
        Ok(())
    }
}

// — Saver ————————————————————————————————————————————————————————————————————

async fn save_batch(batch: &EventBatch, dir: &Path) -> Result<()> {
    if batch.events.is_empty() {
        return Ok(());
    }
    let name = batch.continuation_token.as_deref().unwrap_or("0");
    fs::write(
        dir.join(format!("{name}.json")),
        serde_json::to_string_pretty(batch)?,
    )?;
    Ok(())
}

// — Contract call processor ——————————————————————————————————————————————————

fn is_model_event(sel: Felt) -> bool {
    sel == selector!("ModelRegistered")
        || sel == selector!("EventRegistered")
        || sel == selector!("ModelUpgraded")
        || sel == selector!("EventUpgraded")
}

async fn fetch_and_save_contract(
    address: Felt,
    provider: &JsonRpcClient<HttpTransport>,
    dir: &Path,
) -> Result<()> {
    let path = dir.join(format!("{address:#066x}.json"));
    if path.exists() {
        return Ok(());
    }

    let block = BlockId::Tag(BlockTag::Latest);

    let schema = provider
        .call(
            FunctionCall {
                contract_address: address,
                entry_point_selector: selector!("schema"),
                calldata: vec![],
            },
            block,
        )
        .await?;

    let use_legacy_storage = provider
        .call(
            FunctionCall {
                contract_address: address,
                entry_point_selector: selector!("use_legacy_storage"),
                calldata: vec![],
            },
            block,
        )
        .await
        .ok()
        .and_then(|r| r.first().map(|v| *v != Felt::ZERO));

    fs::write(
        path,
        serde_json::to_string_pretty(&ContractData {
            schema,
            use_legacy_storage,
        })?,
    )?;
    Ok(())
}

async fn process_contract_calls(
    events: &[EmittedEvent],
    provider: &JsonRpcClient<HttpTransport>,
    dir: &Path,
    seen: &mut HashSet<Felt>,
) -> Result<()> {
    for event in events {
        let Some(&sel) = event.keys.first() else {
            continue;
        };
        if !is_model_event(sel) {
            continue;
        }
        // data layout: [class_hash, contract_address, ...]
        let Some(&address) = event.data.get(1) else {
            continue;
        };
        if !seen.insert(address) {
            continue;
        }
        if let Err(e) = fetch_and_save_contract(address, provider, dir).await {
            eprintln!("contract {address:#x}: {e}");
        }
    }
    Ok(())
}

// — Main —————————————————————————————————————————————————————————————————————

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let contract = Felt::from_hex(&args.contract)?;

    let events_dir = args.dir.join("events");
    let contracts_dir = args.dir.join("model-contracts");
    fs::create_dir_all(&events_dir)?;
    fs::create_dir_all(&contracts_dir)?;

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(Url::parse(
        &args.rpc_url,
    )?)));

    let (tx, mut rx) = mpsc::channel::<EventBatch>(8);
    let fetch_task = tokio::spawn(
        EventFetcher {
            provider: provider.clone(),
            contract,
        }
        .run(tx),
    );

    let mut seen_contracts: HashSet<Felt> = HashSet::new();
    let mut n = 0u32;

    while let Some(batch) = rx.recv().await {
        eprintln!(
            "chunk {n} ({}): {} events",
            batch.continuation_token.as_deref().unwrap_or("0"),
            batch.events.len()
        );
        save_batch(&batch, &events_dir).await?;
        process_contract_calls(
            &batch.events,
            &provider,
            &contracts_dir,
            &mut seen_contracts,
        )
        .await?;
        n += 1;
    }

    fetch_task.await??;
    eprintln!(
        "done — {n} chunks, {} contracts saved",
        seen_contracts.len()
    );
    Ok(())
}
