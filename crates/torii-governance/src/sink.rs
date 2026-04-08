use crate::decoder::{
    DelegateChanged, DelegateVotesChanged, ProposalCanceled, ProposalCreated, ProposalExecuted,
    ProposalQueued, VoteCast, VoteCastWithParams,
};
use crate::discovery::{
    fetch_current_votes, fetch_governance_metadata, fetch_proposal_metadata,
    register_governor_and_votes_token,
};
use crate::grpc_service::GovernanceService;
use crate::proto;
use crate::storage::{
    DelegationRecord, GovernanceStorage, GovernorRecord, ProposalRecord, VoteRecord,
    VotingPowerRecord, VotingPowerSource,
};
use async_trait::async_trait;
use axum::Router;
use prost::Message;
use prost_types::Any;
use starknet::core::types::Felt;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use torii::etl::decoder::DecoderId;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii::etl::{EngineDb, Envelope, TypeId};
use torii::grpc::UpdateType;
use torii_common::u256_to_bytes;
use torii_erc20::Erc20Storage;

pub struct GovernanceSink {
    storage: Arc<GovernanceStorage>,
    provider: Arc<JsonRpcClient<HttpTransport>>,
    engine_db: Arc<EngineDb>,
    registry_cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
    erc20_storage: Option<Arc<Erc20Storage>>,
    grpc_service: Option<GovernanceService>,
    event_bus: Option<Arc<EventBus>>,
    bootstrapped_governors: Mutex<std::collections::HashSet<Felt>>,
}

impl GovernanceSink {
    pub fn new(
        storage: Arc<GovernanceStorage>,
        provider: Arc<JsonRpcClient<HttpTransport>>,
        engine_db: Arc<EngineDb>,
        registry_cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
    ) -> Self {
        Self {
            storage,
            provider,
            engine_db,
            registry_cache,
            erc20_storage: None,
            grpc_service: None,
            event_bus: None,
            bootstrapped_governors: Mutex::new(std::collections::HashSet::new()),
        }
    }

    pub fn with_grpc_service(mut self, grpc_service: GovernanceService) -> Self {
        self.grpc_service = Some(grpc_service);
        self
    }

    pub fn with_erc20_storage(mut self, erc20_storage: Arc<Erc20Storage>) -> Self {
        self.erc20_storage = Some(erc20_storage);
        self
    }

    async fn ensure_governor_bootstrapped(&self, governor: Felt) {
        {
            let seen = self.bootstrapped_governors.lock().await;
            if seen.contains(&governor) {
                return;
            }
        }

        match fetch_governance_metadata(self.provider.as_ref(), governor).await {
            Ok(metadata) => {
                let _ = register_governor_and_votes_token(
                    self.engine_db.as_ref(),
                    &self.registry_cache,
                    &metadata,
                )
                .await;
                let _ = self
                    .storage
                    .upsert_governor(&GovernorRecord {
                        address: metadata.governor,
                        votes_token: Some(metadata.votes_token),
                        name: metadata.name,
                        version: metadata.version,
                        src5_supported: metadata.src5_supported,
                        governor_interface_supported: metadata.governor_interface_supported,
                    })
                    .await;
                self.bootstrapped_governors.lock().await.insert(governor);
            }
            Err(error) => {
                tracing::debug!(
                    target: "torii_governance::sink",
                    governor = %format!("{governor:#x}"),
                    error = %error,
                    "Governance bootstrap skipped for contract"
                );
            }
        }
    }

    async fn refresh_voting_power(
        &self,
        votes_token: Felt,
        account: Felt,
        delegate: Option<Felt>,
        block_number: u64,
        tx_hash: Felt,
    ) {
        if let Ok(voting_power) =
            fetch_current_votes(self.provider.as_ref(), votes_token, account).await
        {
            let _ = self
                .storage
                .upsert_voting_power(&VotingPowerRecord {
                    votes_token,
                    account,
                    delegate,
                    voting_power,
                    source: VotingPowerSource::RpcGetVotes,
                    last_block: block_number,
                    last_tx_hash: tx_hash,
                })
                .await;
            return;
        }

        if let Some(erc20_storage) = &self.erc20_storage {
            if let Ok(Some(balance)) = erc20_storage.get_balance(votes_token, account).await {
                let _ = self
                    .storage
                    .upsert_voting_power(&VotingPowerRecord {
                        votes_token,
                        account,
                        delegate,
                        voting_power: balance,
                        source: VotingPowerSource::Erc20Balance,
                        last_block: block_number,
                        last_tx_hash: tx_hash,
                    })
                    .await;
            }
        }
    }

    async fn hydrate_proposal_record(
        &self,
        governor: Felt,
        proposal_id: Felt,
        existing: Option<&ProposalRecord>,
        fallback_proposer: Option<Felt>,
    ) -> (Option<Felt>, u64, u64) {
        let metadata = fetch_proposal_metadata(self.provider.as_ref(), governor, proposal_id).await;
        let proposer = metadata
            .proposer
            .or(fallback_proposer)
            .or(existing.and_then(|value| value.proposer));
        let snapshot = metadata
            .snapshot
            .or(existing
                .map(|value| value.snapshot)
                .filter(|value| *value != 0))
            .unwrap_or(0);
        let deadline = metadata
            .deadline
            .or(existing
                .map(|value| value.deadline)
                .filter(|value| *value != 0))
            .unwrap_or(0);

        (proposer, snapshot, deadline)
    }

    fn proposal_proto(record: &ProposalRecord) -> proto::Proposal {
        proto::Proposal {
            governor: record.governor.to_bytes_be().to_vec(),
            proposal_id: record.proposal_id.to_bytes_be().to_vec(),
            proposer: record.proposer.map(|value| value.to_bytes_be().to_vec()),
            description: record.description.clone(),
            snapshot: record.snapshot,
            deadline: record.deadline,
            status: record.status.clone(),
            created_block: record.created_block,
            created_tx_hash: record.created_tx_hash.to_bytes_be().to_vec(),
            created_timestamp: record.created_timestamp.unwrap_or(0),
            queued_block: record.queued_block,
            executed_block: record.executed_block,
            canceled_block: record.canceled_block,
        }
    }

    fn vote_proto(record: &VoteRecord) -> proto::Vote {
        proto::Vote {
            governor: record.governor.to_bytes_be().to_vec(),
            proposal_id: record.proposal_id.to_bytes_be().to_vec(),
            voter: record.voter.to_bytes_be().to_vec(),
            support: u32::from(record.support),
            weight: u256_to_bytes(record.weight),
            reason: record.reason.clone(),
            params: record.params.clone(),
            block_number: record.block_number,
            tx_hash: record.tx_hash.to_bytes_be().to_vec(),
            timestamp: record.timestamp.unwrap_or(0),
        }
    }

    fn delegation_proto(record: &DelegationRecord) -> proto::Delegation {
        proto::Delegation {
            votes_token: record.votes_token.to_bytes_be().to_vec(),
            delegator: record.delegator.to_bytes_be().to_vec(),
            from_delegate: record.from_delegate.to_bytes_be().to_vec(),
            to_delegate: record.to_delegate.to_bytes_be().to_vec(),
            block_number: record.block_number,
            tx_hash: record.tx_hash.to_bytes_be().to_vec(),
            timestamp: record.timestamp.unwrap_or(0),
        }
    }

    fn publish_proposal(&self, proposal: &proto::Proposal) {
        if let Some(event_bus) = &self.event_bus {
            let mut buf = Vec::new();
            if proposal.encode(&mut buf).is_ok() {
                event_bus.publish_protobuf(
                    "governance.proposals",
                    "governance.proposal",
                    &Any {
                        type_url: "type.googleapis.com/torii.sinks.governance.Proposal".to_string(),
                        value: buf,
                    },
                    proposal,
                    UpdateType::Created,
                    |_proposal: &proto::Proposal, _filters| true,
                );
            }
        }
        if let Some(service) = &self.grpc_service {
            service.broadcast_proposal(proposal.clone());
        }
    }

    fn publish_vote(&self, vote: &proto::Vote) {
        if let Some(service) = &self.grpc_service {
            service.broadcast_vote(vote.clone());
        }
    }

    fn publish_delegation(&self, delegation: &proto::Delegation) {
        if let Some(service) = &self.grpc_service {
            service.broadcast_delegation(delegation.clone());
        }
    }
}

#[async_trait]
impl Sink for GovernanceSink {
    fn name(&self) -> &'static str {
        "governance-sink"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("governance.proposal_created"),
            TypeId::new("governance.proposal_queued"),
            TypeId::new("governance.proposal_executed"),
            TypeId::new("governance.proposal_canceled"),
            TypeId::new("governance.vote_cast"),
            TypeId::new("governance.vote_cast_with_params"),
            TypeId::new("governance.delegate_changed"),
            TypeId::new("governance.delegate_votes_changed"),
        ]
    }

    async fn process(
        &self,
        envelopes: &[Envelope],
        batch: &torii::etl::extractor::ExtractionBatch,
    ) -> anyhow::Result<()> {
        for envelope in envelopes {
            if let Some(event) = envelope.body.as_any().downcast_ref::<ProposalCreated>() {
                self.ensure_governor_bootstrapped(event.governor).await;
                let existing = self
                    .storage
                    .get_proposal(event.governor, event.proposal_id)
                    .await?;
                let (proposer, snapshot, deadline) = self
                    .hydrate_proposal_record(
                        event.governor,
                        event.proposal_id,
                        existing.as_ref(),
                        Some(event.proposer),
                    )
                    .await;
                let record = ProposalRecord {
                    id: existing.as_ref().map_or(0, |value| value.id),
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    proposer,
                    description: existing
                        .as_ref()
                        .and_then(|value| value.description.clone()),
                    snapshot,
                    deadline,
                    status: "created".to_string(),
                    created_block: event.block_number,
                    created_tx_hash: event.transaction_hash,
                    created_timestamp: batch
                        .blocks
                        .get(&event.block_number)
                        .map(|block| block.timestamp as i64),
                    queued_block: existing.as_ref().and_then(|value| value.queued_block),
                    executed_block: existing.as_ref().and_then(|value| value.executed_block),
                    canceled_block: existing.as_ref().and_then(|value| value.canceled_block),
                };
                self.storage.upsert_proposal(&record).await?;
                self.publish_proposal(&Self::proposal_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<ProposalQueued>() {
                let existing = self
                    .storage
                    .get_proposal(event.governor, event.proposal_id)
                    .await?;
                let (proposer, snapshot, deadline) = self
                    .hydrate_proposal_record(
                        event.governor,
                        event.proposal_id,
                        existing.as_ref(),
                        None,
                    )
                    .await;
                let record = ProposalRecord {
                    id: existing.as_ref().map_or(0, |value| value.id),
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    proposer,
                    description: existing
                        .as_ref()
                        .and_then(|value| value.description.clone()),
                    snapshot,
                    deadline,
                    status: "queued".to_string(),
                    created_block: existing
                        .as_ref()
                        .map_or(event.block_number, |value| value.created_block),
                    created_tx_hash: existing
                        .as_ref()
                        .map_or(event.transaction_hash, |value| value.created_tx_hash),
                    created_timestamp: existing.as_ref().and_then(|value| value.created_timestamp),
                    queued_block: Some(event.block_number),
                    executed_block: existing.as_ref().and_then(|value| value.executed_block),
                    canceled_block: existing.as_ref().and_then(|value| value.canceled_block),
                };
                self.storage.upsert_proposal(&record).await?;
                self.publish_proposal(&Self::proposal_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<ProposalExecuted>() {
                let existing = self
                    .storage
                    .get_proposal(event.governor, event.proposal_id)
                    .await?;
                let (proposer, snapshot, deadline) = self
                    .hydrate_proposal_record(
                        event.governor,
                        event.proposal_id,
                        existing.as_ref(),
                        None,
                    )
                    .await;
                let record = ProposalRecord {
                    id: existing.as_ref().map_or(0, |value| value.id),
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    proposer,
                    description: existing
                        .as_ref()
                        .and_then(|value| value.description.clone()),
                    snapshot,
                    deadline,
                    status: "executed".to_string(),
                    created_block: existing
                        .as_ref()
                        .map_or(event.block_number, |value| value.created_block),
                    created_tx_hash: existing
                        .as_ref()
                        .map_or(event.transaction_hash, |value| value.created_tx_hash),
                    created_timestamp: existing.as_ref().and_then(|value| value.created_timestamp),
                    queued_block: existing.as_ref().and_then(|value| value.queued_block),
                    executed_block: Some(event.block_number),
                    canceled_block: existing.as_ref().and_then(|value| value.canceled_block),
                };
                self.storage.upsert_proposal(&record).await?;
                self.publish_proposal(&Self::proposal_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<ProposalCanceled>() {
                let existing = self
                    .storage
                    .get_proposal(event.governor, event.proposal_id)
                    .await?;
                let (proposer, snapshot, deadline) = self
                    .hydrate_proposal_record(
                        event.governor,
                        event.proposal_id,
                        existing.as_ref(),
                        None,
                    )
                    .await;
                let record = ProposalRecord {
                    id: existing.as_ref().map_or(0, |value| value.id),
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    proposer,
                    description: existing
                        .as_ref()
                        .and_then(|value| value.description.clone()),
                    snapshot,
                    deadline,
                    status: "canceled".to_string(),
                    created_block: existing
                        .as_ref()
                        .map_or(event.block_number, |value| value.created_block),
                    created_tx_hash: existing
                        .as_ref()
                        .map_or(event.transaction_hash, |value| value.created_tx_hash),
                    created_timestamp: existing.as_ref().and_then(|value| value.created_timestamp),
                    queued_block: existing.as_ref().and_then(|value| value.queued_block),
                    executed_block: existing.as_ref().and_then(|value| value.executed_block),
                    canceled_block: Some(event.block_number),
                };
                self.storage.upsert_proposal(&record).await?;
                self.publish_proposal(&Self::proposal_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<VoteCast>() {
                let record = VoteRecord {
                    id: 0,
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    voter: event.voter,
                    support: event.support,
                    weight: event.weight,
                    reason: event.reason.clone(),
                    params: Vec::new(),
                    block_number: event.block_number,
                    tx_hash: event.transaction_hash,
                    timestamp: batch
                        .blocks
                        .get(&event.block_number)
                        .map(|block| block.timestamp as i64),
                };
                self.storage.insert_vote(&record).await?;
                self.publish_vote(&Self::vote_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<VoteCastWithParams>()
            {
                let record = VoteRecord {
                    id: 0,
                    governor: event.governor,
                    proposal_id: event.proposal_id,
                    voter: event.voter,
                    support: event.support,
                    weight: event.weight,
                    reason: event.reason.clone(),
                    params: serde_json::to_vec(
                        &event
                            .params
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap_or_default(),
                    block_number: event.block_number,
                    tx_hash: event.transaction_hash,
                    timestamp: batch
                        .blocks
                        .get(&event.block_number)
                        .map(|block| block.timestamp as i64),
                };
                self.storage.insert_vote(&record).await?;
                self.publish_vote(&Self::vote_proto(&record));
            } else if let Some(event) = envelope.body.as_any().downcast_ref::<DelegateChanged>() {
                let record = DelegationRecord {
                    id: 0,
                    votes_token: event.votes_token,
                    delegator: event.delegator,
                    from_delegate: event.from_delegate,
                    to_delegate: event.to_delegate,
                    block_number: event.block_number,
                    tx_hash: event.transaction_hash,
                    timestamp: batch
                        .blocks
                        .get(&event.block_number)
                        .map(|block| block.timestamp as i64),
                };
                self.storage.insert_delegation(&record).await?;
                self.refresh_voting_power(
                    event.votes_token,
                    event.delegator,
                    Some(event.to_delegate),
                    event.block_number,
                    event.transaction_hash,
                )
                .await;
                self.publish_delegation(&Self::delegation_proto(&record));
            } else if let Some(event) = envelope
                .body
                .as_any()
                .downcast_ref::<DelegateVotesChanged>()
            {
                self.storage
                    .upsert_voting_power(&VotingPowerRecord {
                        votes_token: event.votes_token,
                        account: event.delegate,
                        delegate: Some(event.delegate),
                        voting_power: event.new_votes,
                        source: VotingPowerSource::DelegateVotesChanged,
                        last_block: event.block_number,
                        last_tx_hash: event.transaction_hash,
                    })
                    .await?;
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "governance.proposals",
                vec!["governor".to_string()],
                "Governance proposal updates",
            ),
            TopicInfo::new(
                "governance.votes",
                vec!["governor".to_string()],
                "Governance vote updates",
            ),
            TopicInfo::new(
                "governance.delegations",
                vec!["votes_token".to_string()],
                "Governance delegation updates",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> anyhow::Result<()> {
        self.event_bus = Some(event_bus);
        Ok(())
    }
}
