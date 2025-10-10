//! A simple app to showcase how to work with torii core and test its configuration.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use starknet::macros::selector;
use tokio::time::{sleep, Duration};
use torii_core::{run_once_batch, FetchPlan, Fetcher, FieldElement, Sink, ToriiConfig};
use torii_registry::decoders::{
    self, Erc20DecoderConfig, Erc721DecoderConfig, IntrospectDecoderConfig,
};
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

    let decoder_registry = decoders::from_config(&config.decoders).await?;

    let fetcher: Arc<dyn Fetcher> = if let Some(value) = config.fetcher.as_ref() {
        fetchers::from_config(value).await?
    } else {
        tracing::info!(
            target: "torii_fetcher_jsonrpc",
            "no fetcher configured, falling back to mock events"
        );
        let events = sample_events();
        Arc::new(MockFetcher::new(events))
    };

    let sink_registry = sinks::from_config(&config.sinks).await?;

    let sinks: Vec<Arc<dyn Sink>> = sink_registry.sinks().to_vec();
    if sinks.is_empty() {
        anyhow::bail!("no sinks configured");
    }

    for _ in 0..2 {
        run_once_batch(fetcher.as_ref(), &decoder_registry, &sinks).await?;
        sleep(Duration::from_millis(250)).await;
    }

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

fn sample_events() -> Vec<EmittedEvent> {
    let introspect_cfg = IntrospectDecoderConfig {
        contracts: vec!["0x777".into()],
    };
    let erc20_cfg = Erc20DecoderConfig {
        contracts: vec!["0x42".into()],
    };
    let erc721_cfg = Erc721DecoderConfig {
        contracts: vec!["0x99".into()],
    };

    let select_contract = |contracts: &[String], label: &str| -> FieldElement {
        let hex = contracts
            .first()
            .unwrap_or_else(|| panic!("{label} decoder requires at least one contract"));
        FieldElement::from_hex(hex).unwrap()
    };

    let contract_introspect = select_contract(&introspect_cfg.contracts, "introspect");

    let contract_erc20 = select_contract(&erc20_cfg.contracts, "erc20");
    let erc20_selector = selector!("Transfer");

    let contract_erc721 = select_contract(&erc721_cfg.contracts, "erc721");
    let erc721_selector = selector!("Transfer");

    let block_number = Some(1234);

    vec![
        EmittedEvent {
            from_address: contract_introspect,
            keys: vec![selector!("DeclareTable")],
            data: vec![],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xa11ce").unwrap(),
        },
        EmittedEvent {
            from_address: contract_introspect,
            keys: vec![selector!("SetRecord")],
            data: vec![],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xb0b").unwrap(),
        },
        EmittedEvent {
            from_address: contract_erc20,
            keys: vec![erc20_selector, FieldElement::from_hex("0xf00").unwrap()],
            data: vec![FieldElement::from_hex("0x64").unwrap()],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0xdeadbeef").unwrap(),
        },
        EmittedEvent {
            from_address: contract_erc721,
            keys: vec![
                erc721_selector,
                FieldElement::from_hex("0xca11").unwrap(),
                FieldElement::from_hex("0xfe").unwrap(),
            ],
            data: vec![FieldElement::from_hex("0x1").unwrap()],
            block_hash: None,
            block_number,
            transaction_hash: FieldElement::from_hex("0x600d").unwrap(),
        },
    ]
}
