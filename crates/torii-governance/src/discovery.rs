use anyhow::{anyhow, Result};
use starknet::core::codec::Decode;
use starknet::core::types::{BlockId, BlockTag, ByteArray, Felt, FunctionCall, U256};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Provider;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use torii::etl::decoder::DecoderId;
use torii::etl::EngineDb;

pub const ISRC5_ID: Felt =
    Felt::from_hex_unchecked("0x3f918d17e5ee77373b56385708f855659a07f75997f365cf87748628532a055");
pub const IGOVERNOR_ID: Felt =
    Felt::from_hex_unchecked("0x1100a1f8546595b5bd75a6cd8fcc5b015370655e66f275963321c5cd0357ac9");

pub fn governance_decoder_id() -> DecoderId {
    DecoderId::new("governance")
}

#[derive(Debug, Clone)]
pub struct GovernanceMetadata {
    pub governor: Felt,
    pub votes_token: Felt,
    pub name: Option<String>,
    pub version: Option<String>,
    pub src5_supported: bool,
    pub governor_interface_supported: bool,
}

#[derive(Debug, Clone, Default)]
pub struct GovernanceBootstrap {
    pub governors: Vec<GovernanceMetadata>,
    pub added_votes_tokens: Vec<Felt>,
}

#[derive(Debug, Clone, Default)]
pub struct ProposalMetadata {
    pub proposer: Option<Felt>,
    pub snapshot: Option<u64>,
    pub deadline: Option<u64>,
}

fn decode_string_result(result: &[Felt]) -> Option<String> {
    if result.is_empty() {
        return None;
    }

    if result.len() == 1 {
        return parse_cairo_short_string(&result[0])
            .ok()
            .filter(|value| !value.is_empty());
    }

    if let Ok(byte_array) = ByteArray::decode(result) {
        if let Ok(value) = String::try_from(byte_array) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }

    parse_cairo_short_string(&result[0])
        .ok()
        .filter(|value| !value.is_empty())
}

async fn call_contract(
    provider: &JsonRpcClient<HttpTransport>,
    contract_address: Felt,
    entry_point_selector: Felt,
    calldata: Vec<Felt>,
) -> Result<Vec<Felt>> {
    provider
        .call(
            FunctionCall {
                contract_address,
                entry_point_selector,
                calldata,
            },
            BlockId::Tag(BlockTag::Latest),
        )
        .await
        .map_err(|error| anyhow!(error))
}

fn parse_u64_result(result: &[Felt]) -> Option<u64> {
    result.first().and_then(|value| (*value).try_into().ok())
}

fn parse_felt_result(result: &[Felt]) -> Option<Felt> {
    result.first().copied()
}

pub async fn fetch_current_votes(
    provider: &JsonRpcClient<HttpTransport>,
    votes_token: Felt,
    account: Felt,
) -> Result<U256> {
    let response =
        call_contract(provider, votes_token, selector!("get_votes"), vec![account]).await?;
    Ok(match response.as_slice() {
        [] => U256::from(0u64),
        [single] => {
            let bytes = single.to_bytes_be();
            U256::from(u128::from_be_bytes(bytes[16..32].try_into().unwrap()))
        }
        [low, high, ..] => U256::from_words(
            u128::from_be_bytes(low.to_bytes_be()[16..32].try_into().unwrap()),
            u128::from_be_bytes(high.to_bytes_be()[16..32].try_into().unwrap()),
        ),
    })
}

pub async fn fetch_proposal_metadata(
    provider: &JsonRpcClient<HttpTransport>,
    governor: Felt,
    proposal_id: Felt,
) -> ProposalMetadata {
    let proposer = call_contract(
        provider,
        governor,
        selector!("proposal_proposer"),
        vec![proposal_id],
    )
    .await
    .ok()
    .and_then(|result| parse_felt_result(&result));
    let snapshot = call_contract(
        provider,
        governor,
        selector!("proposal_snapshot"),
        vec![proposal_id],
    )
    .await
    .ok()
    .and_then(|result| parse_u64_result(&result));
    let deadline = call_contract(
        provider,
        governor,
        selector!("proposal_deadline"),
        vec![proposal_id],
    )
    .await
    .ok()
    .and_then(|result| parse_u64_result(&result));

    ProposalMetadata {
        proposer,
        snapshot,
        deadline,
    }
}

async fn supports_interface(
    provider: &JsonRpcClient<HttpTransport>,
    contract: Felt,
    interface_id: Felt,
) -> Result<bool> {
    let response = call_contract(
        provider,
        contract,
        selector!("supports_interface"),
        vec![interface_id],
    )
    .await?;

    Ok(response.first().copied().unwrap_or(Felt::ZERO) != Felt::ZERO)
}

async fn fetch_votes_token(
    provider: &JsonRpcClient<HttpTransport>,
    governor: Felt,
) -> Result<Felt> {
    let response = call_contract(provider, governor, selector!("token"), Vec::new()).await?;
    response
        .first()
        .copied()
        .ok_or_else(|| anyhow!("token() returned no address"))
}

pub async fn fetch_governance_metadata(
    provider: &JsonRpcClient<HttpTransport>,
    governor: Felt,
) -> Result<GovernanceMetadata> {
    let src5_supported = supports_interface(provider, governor, ISRC5_ID)
        .await
        .unwrap_or(false);
    let governor_interface_supported = supports_interface(provider, governor, IGOVERNOR_ID)
        .await
        .unwrap_or(false);

    if !src5_supported || !governor_interface_supported {
        return Err(anyhow!("governor does not support required interfaces"));
    }

    let votes_token = fetch_votes_token(provider, governor).await?;
    let name = call_contract(provider, governor, selector!("name"), Vec::new())
        .await
        .ok()
        .and_then(|value| decode_string_result(&value));
    let version = call_contract(provider, governor, selector!("version"), Vec::new())
        .await
        .ok()
        .and_then(|value| decode_string_result(&value));

    Ok(GovernanceMetadata {
        governor,
        votes_token,
        name,
        version,
        src5_supported,
        governor_interface_supported,
    })
}

#[allow(clippy::implicit_hasher)]
pub async fn register_governor_and_votes_token(
    engine_db: &EngineDb,
    registry_cache: &Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
    metadata: &GovernanceMetadata,
) -> Result<bool> {
    let mut added_votes_token = false;

    {
        let mut cache = registry_cache.write().await;
        cache.insert(metadata.governor, vec![governance_decoder_id()]);

        let token_decoders = vec![DecoderId::new("erc20"), governance_decoder_id()];
        let existing = cache
            .get(&metadata.votes_token)
            .cloned()
            .unwrap_or_default();
        if existing != token_decoders {
            cache.insert(metadata.votes_token, token_decoders);
            added_votes_token = true;
        }
    }

    engine_db
        .set_contract_decoders(metadata.governor, &[governance_decoder_id()])
        .await?;
    engine_db
        .set_contract_decoders(
            metadata.votes_token,
            &[DecoderId::new("erc20"), governance_decoder_id()],
        )
        .await?;

    Ok(added_votes_token)
}

#[allow(clippy::implicit_hasher)]
pub async fn bootstrap_governance_registry(
    provider: Arc<JsonRpcClient<HttpTransport>>,
    engine_db: Arc<EngineDb>,
    registry_cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
) -> Result<GovernanceBootstrap> {
    let mappings = engine_db.get_all_contract_decoders().await?;
    let mut bootstrap = GovernanceBootstrap::default();
    let mut seen_votes_tokens = HashSet::new();

    for (contract, decoder_ids, _) in mappings {
        if !decoder_ids.contains(&governance_decoder_id()) {
            continue;
        }

        let metadata = match fetch_governance_metadata(provider.as_ref(), contract).await {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::debug!(
                    target: "torii_governance::discovery",
                    governor = %format!("{contract:#x}"),
                    error = %error,
                    "Skipping governance candidate during bootstrap"
                );
                continue;
            }
        };

        if register_governor_and_votes_token(engine_db.as_ref(), &registry_cache, &metadata).await?
            && seen_votes_tokens.insert(metadata.votes_token)
        {
            bootstrap.added_votes_tokens.push(metadata.votes_token);
        }

        bootstrap.governors.push(metadata);
    }

    Ok(bootstrap)
}
