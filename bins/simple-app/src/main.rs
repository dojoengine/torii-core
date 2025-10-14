//! A simple app to showcase how to work with torii core and test its configuration.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use starknet::macros::selector;
use tokio::time::{sleep, Duration};
use torii_core::{run_once_batch, FetchPlan, Fetcher, FieldElement, Sink, ToriiConfig};
use torii_registry::decoders;
use torii_registry::{fetchers, sinks};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "torii.toml".to_string());

    let config = ToriiConfig::new(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;
    tracing::debug!(
        target: "torii_core",
        config_path,
        config = ?config,
        "Loading Torii configuration"
    );

    let decoder_registry = decoders::from_config(&config.decoders, &config.contracts).await?;

    let fetcher: Arc<dyn Fetcher> = if let Some(value) = config.fetcher.as_ref() {
        fetchers::from_config(value).await?
    } else {
        tracing::info!(
            target: "torii_fetcher_jsonrpc",
            "no fetcher configured, falling back to mock events"
        );
        let events = sample_events(&config)?;
        Arc::new(MockFetcher::new(events))
    };

    let sink_registry = sinks::from_config(&config.sinks, &config.contracts).await?;

    let sinks: Vec<Arc<dyn Sink>> = sink_registry.sinks().to_vec();
    if sinks.is_empty() {
        anyhow::bail!("no sinks configured");
    }

    run_once_batch(fetcher.as_ref(), &decoder_registry, &sinks).await?;
    sleep(Duration::from_millis(250)).await;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "torii_fetcher_jsonrpc=info,torii_decoders=info,torii_sinks=info,torii_registry=info,torii_core=info,info",
        )
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .with_target(true)
        .init();
}

struct MockFetcher {
    events: Vec<EmittedEvent>,
}

impl MockFetcher {
    fn new(events: Vec<EmittedEvent>) -> Self {
        Self { events }
    }
}

#[async_trait]
impl Fetcher for MockFetcher {
    async fn fetch(&self, plan: &FetchPlan) -> Result<Vec<EmittedEvent>> {
        tracing::info!(
            addresses = plan.contract_addresses.len(),
            selectors = plan.selectors.len(),
            "mock fetcher returning synthetic events"
        );
        Ok(self.events.clone())
    }
}

fn sample_events(config: &ToriiConfig) -> Result<Vec<EmittedEvent>> {
    use std::collections::HashMap;

    let mut addresses: HashMap<String, FieldElement> = HashMap::new();
    for (name, binding) in &config.contracts {
        let address = FieldElement::from_hex(&binding.address)
            .with_context(|| format!("invalid contract address '{name}': {}", binding.address))?;
        for decoder in &binding.decoders {
            addresses.entry(decoder.clone()).or_insert(address);
        }
    }

    let mut events = Vec::new();
    let block_number = Some(1234);

    if let Some(contract) = addresses.get("introspect") {
        let contract = *contract;
        events.push(EmittedEvent {
            from_address: contract,
            keys: vec![selector!("DeclareTable")],
            data: vec![],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xa11ce").unwrap(),
        });
        events.push(EmittedEvent {
            from_address: contract,
            keys: vec![selector!("SetRecord")],
            data: vec![],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xb0b").unwrap(),
        });
    }

    if let Some(contract) = addresses.get("erc20") {
        let contract = *contract;
        let erc20_selector = selector!("Transfer");
        events.push(EmittedEvent {
            from_address: contract,
            keys: vec![erc20_selector, FieldElement::from_hex("0xf00").unwrap()],
            data: vec![FieldElement::from_hex("0x64").unwrap()],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xdeadbeef").unwrap(),
        });
    }

    if let Some(contract) = addresses.get("erc721") {
        let contract = *contract;
        let erc721_selector = selector!("Transfer");
        events.push(EmittedEvent {
            from_address: contract,
            keys: vec![
                erc721_selector,
                FieldElement::from_hex("0xca11").unwrap(),
                FieldElement::from_hex("0xfe").unwrap(),
            ],
            data: vec![FieldElement::from_hex("0x1").unwrap()],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0x600d").unwrap(),
        });
    }

    Ok(events)
}
