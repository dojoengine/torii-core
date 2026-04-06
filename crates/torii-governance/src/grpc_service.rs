use crate::proto::{
    governance_server::Governance as GovernanceTrait, Cursor, Delegation, DelegationFilter,
    DelegationUpdate, GetDelegationsRequest, GetDelegationsResponse, GetGovernorsRequest,
    GetGovernorsResponse, GetProposalRequest, GetProposalResponse, GetProposalsRequest,
    GetProposalsResponse, GetVotesRequest, GetVotesResponse, GetVotingPowerRequest,
    GetVotingPowerResponse, Governor, Proposal, ProposalFilter, ProposalUpdate,
    SubscribeDelegationsRequest, SubscribeProposalsRequest, SubscribeVotesRequest, Vote,
    VoteFilter, VoteUpdate, VotingPower, VotingPowerSource,
};
use crate::storage::{
    DelegationCursor, DelegationRecord, GovernanceStorage, GovernorRecord, ProposalCursor,
    ProposalRecord, VoteCursor, VoteRecord, VotingPowerRecord,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use torii_common::bytes_to_felt;

#[derive(Clone)]
pub struct GovernanceService {
    storage: Arc<GovernanceStorage>,
    proposal_tx: broadcast::Sender<ProposalUpdate>,
    vote_tx: broadcast::Sender<VoteUpdate>,
    delegation_tx: broadcast::Sender<DelegationUpdate>,
}

impl GovernanceService {
    pub fn new(storage: Arc<GovernanceStorage>) -> Self {
        let (proposal_tx, _) = broadcast::channel(1000);
        let (vote_tx, _) = broadcast::channel(1000);
        let (delegation_tx, _) = broadcast::channel(1000);
        Self {
            storage,
            proposal_tx,
            vote_tx,
            delegation_tx,
        }
    }

    pub fn broadcast_proposal(&self, proposal: Proposal) {
        let _ = self.proposal_tx.send(ProposalUpdate {
            proposal: Some(proposal),
            timestamp: chrono::Utc::now().timestamp(),
        });
    }

    pub fn broadcast_vote(&self, vote: Vote) {
        let _ = self.vote_tx.send(VoteUpdate {
            vote: Some(vote),
            timestamp: chrono::Utc::now().timestamp(),
        });
    }

    pub fn broadcast_delegation(&self, delegation: Delegation) {
        let _ = self.delegation_tx.send(DelegationUpdate {
            delegation: Some(delegation),
            timestamp: chrono::Utc::now().timestamp(),
        });
    }

    fn governor_to_proto(record: GovernorRecord) -> Governor {
        Governor {
            address: record.address.to_bytes_be().to_vec(),
            votes_token: record.votes_token.map(|value| value.to_bytes_be().to_vec()),
            name: record.name,
            version: record.version,
            src5_supported: record.src5_supported,
            governor_interface_supported: record.governor_interface_supported,
        }
    }

    fn proposal_to_proto(record: ProposalRecord) -> Proposal {
        Proposal {
            governor: record.governor.to_bytes_be().to_vec(),
            proposal_id: record.proposal_id.to_bytes_be().to_vec(),
            proposer: record.proposer.map(|value| value.to_bytes_be().to_vec()),
            description: record.description,
            snapshot: record.snapshot,
            deadline: record.deadline,
            status: record.status,
            created_block: record.created_block,
            created_tx_hash: record.created_tx_hash.to_bytes_be().to_vec(),
            created_timestamp: record.created_timestamp.unwrap_or(0),
            queued_block: record.queued_block,
            executed_block: record.executed_block,
            canceled_block: record.canceled_block,
        }
    }

    fn vote_to_proto(record: VoteRecord) -> Vote {
        Vote {
            governor: record.governor.to_bytes_be().to_vec(),
            proposal_id: record.proposal_id.to_bytes_be().to_vec(),
            voter: record.voter.to_bytes_be().to_vec(),
            support: u32::from(record.support),
            weight: torii_common::u256_to_bytes(record.weight),
            reason: record.reason,
            params: record.params,
            block_number: record.block_number,
            tx_hash: record.tx_hash.to_bytes_be().to_vec(),
            timestamp: record.timestamp.unwrap_or(0),
        }
    }

    fn delegation_to_proto(record: DelegationRecord) -> Delegation {
        Delegation {
            votes_token: record.votes_token.to_bytes_be().to_vec(),
            delegator: record.delegator.to_bytes_be().to_vec(),
            from_delegate: record.from_delegate.to_bytes_be().to_vec(),
            to_delegate: record.to_delegate.to_bytes_be().to_vec(),
            block_number: record.block_number,
            tx_hash: record.tx_hash.to_bytes_be().to_vec(),
            timestamp: record.timestamp.unwrap_or(0),
        }
    }

    fn voting_power_to_proto(record: VotingPowerRecord) -> VotingPower {
        VotingPower {
            votes_token: record.votes_token.to_bytes_be().to_vec(),
            account: record.account.to_bytes_be().to_vec(),
            delegate: record.delegate.map(|value| value.to_bytes_be().to_vec()),
            voting_power: torii_common::u256_to_bytes(record.voting_power),
            source: match record.source {
                crate::storage::VotingPowerSource::DelegateVotesChanged => {
                    VotingPowerSource::DelegateVotesChanged as i32
                }
                crate::storage::VotingPowerSource::RpcGetVotes => {
                    VotingPowerSource::RpcGetVotes as i32
                }
                crate::storage::VotingPowerSource::Erc20Balance => {
                    VotingPowerSource::Erc20Balance as i32
                }
            },
            last_block: record.last_block,
            last_tx_hash: record.last_tx_hash.to_bytes_be().to_vec(),
        }
    }

    fn matches_proposal_filter(proposal: &Proposal, filter: &ProposalFilter) -> bool {
        if let Some(governor) = &filter.governor {
            if proposal.governor != *governor {
                return false;
            }
        }
        if let Some(proposer) = &filter.proposer {
            if proposal.proposer.as_ref() != Some(proposer) {
                return false;
            }
        }
        if let Some(status) = &filter.status {
            if proposal.status != *status {
                return false;
            }
        }
        true
    }

    fn matches_vote_filter(vote: &Vote, filter: &VoteFilter) -> bool {
        if let Some(governor) = &filter.governor {
            if vote.governor != *governor {
                return false;
            }
        }
        if let Some(proposal_id) = &filter.proposal_id {
            if vote.proposal_id != *proposal_id {
                return false;
            }
        }
        if let Some(voter) = &filter.voter {
            if vote.voter != *voter {
                return false;
            }
        }
        true
    }

    fn matches_delegation_filter(delegation: &Delegation, filter: &DelegationFilter) -> bool {
        if let Some(votes_token) = &filter.votes_token {
            if delegation.votes_token != *votes_token {
                return false;
            }
        }
        if let Some(delegator) = &filter.delegator {
            if delegation.delegator != *delegator {
                return false;
            }
        }
        if let Some(delegatee) = &filter.delegatee {
            if delegation.to_delegate != *delegatee {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl GovernanceTrait for GovernanceService {
    type SubscribeProposalsStream =
        Pin<Box<dyn Stream<Item = Result<ProposalUpdate, Status>> + Send>>;
    type SubscribeVotesStream = Pin<Box<dyn Stream<Item = Result<VoteUpdate, Status>> + Send>>;
    type SubscribeDelegationsStream =
        Pin<Box<dyn Stream<Item = Result<DelegationUpdate, Status>> + Send>>;

    async fn get_governors(
        &self,
        request: Request<GetGovernorsRequest>,
    ) -> Result<Response<GetGovernorsResponse>, Status> {
        let addresses = request
            .into_inner()
            .filter
            .unwrap_or_default()
            .addresses
            .into_iter()
            .filter_map(|value| bytes_to_felt(&value))
            .collect::<Vec<_>>();
        let governors = self
            .storage
            .list_governors(&addresses)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(Self::governor_to_proto)
            .collect();
        Ok(Response::new(GetGovernorsResponse { governors }))
    }

    async fn get_proposals(
        &self,
        request: Request<GetProposalsRequest>,
    ) -> Result<Response<GetProposalsResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };
        let (proposals, next_cursor) = self
            .storage
            .get_proposals_filtered(
                filter
                    .governor
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter
                    .proposer
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter.status.as_deref(),
                req.cursor.map(|cursor| ProposalCursor { id: cursor.id }),
                limit,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetProposalsResponse {
            proposals: proposals.into_iter().map(Self::proposal_to_proto).collect(),
            next_cursor: next_cursor.map(|cursor| Cursor { id: cursor.id }),
        }))
    }

    async fn get_proposal(
        &self,
        request: Request<GetProposalRequest>,
    ) -> Result<Response<GetProposalResponse>, Status> {
        let req = request.into_inner();
        let governor = bytes_to_felt(&req.governor)
            .ok_or_else(|| Status::invalid_argument("invalid governor"))?;
        let proposal_id = bytes_to_felt(&req.proposal_id)
            .ok_or_else(|| Status::invalid_argument("invalid proposal_id"))?;
        let proposal = self
            .storage
            .get_proposal(governor, proposal_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Self::proposal_to_proto);
        Ok(Response::new(GetProposalResponse { proposal }))
    }

    async fn get_votes(
        &self,
        request: Request<GetVotesRequest>,
    ) -> Result<Response<GetVotesResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };
        let (votes, next_cursor) = self
            .storage
            .get_votes_filtered(
                filter
                    .governor
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter
                    .proposal_id
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter.voter.as_ref().and_then(|value| bytes_to_felt(value)),
                req.cursor.map(|cursor| VoteCursor { id: cursor.id }),
                limit,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetVotesResponse {
            votes: votes.into_iter().map(Self::vote_to_proto).collect(),
            next_cursor: next_cursor.map(|cursor| Cursor { id: cursor.id }),
        }))
    }

    async fn get_delegations(
        &self,
        request: Request<GetDelegationsRequest>,
    ) -> Result<Response<GetDelegationsResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };
        let (delegations, next_cursor) = self
            .storage
            .get_delegations_filtered(
                filter
                    .votes_token
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter
                    .delegator
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter
                    .delegatee
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                req.cursor.map(|cursor| DelegationCursor { id: cursor.id }),
                limit,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetDelegationsResponse {
            delegations: delegations
                .into_iter()
                .map(Self::delegation_to_proto)
                .collect(),
            next_cursor: next_cursor.map(|cursor| Cursor { id: cursor.id }),
        }))
    }

    async fn get_voting_power(
        &self,
        request: Request<GetVotingPowerRequest>,
    ) -> Result<Response<GetVotingPowerResponse>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let entries = self
            .storage
            .get_voting_power(
                filter
                    .votes_token
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
                filter
                    .account
                    .as_ref()
                    .and_then(|value| bytes_to_felt(value)),
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(Self::voting_power_to_proto)
            .collect();
        Ok(Response::new(GetVotingPowerResponse { entries }))
    }

    async fn subscribe_proposals(
        &self,
        request: Request<SubscribeProposalsRequest>,
    ) -> Result<Response<Self::SubscribeProposalsStream>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let mut rx = self.proposal_tx.subscribe();
        Ok(Response::new(Box::pin(stream! {
            while let Ok(update) = rx.recv().await {
                if let Some(proposal) = &update.proposal {
                    if Self::matches_proposal_filter(proposal, &filter) {
                        yield Ok(update);
                    }
                }
            }
        })))
    }

    async fn subscribe_votes(
        &self,
        request: Request<SubscribeVotesRequest>,
    ) -> Result<Response<Self::SubscribeVotesStream>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let mut rx = self.vote_tx.subscribe();
        Ok(Response::new(Box::pin(stream! {
            while let Ok(update) = rx.recv().await {
                if let Some(vote) = &update.vote {
                    if Self::matches_vote_filter(vote, &filter) {
                        yield Ok(update);
                    }
                }
            }
        })))
    }

    async fn subscribe_delegations(
        &self,
        request: Request<SubscribeDelegationsRequest>,
    ) -> Result<Response<Self::SubscribeDelegationsStream>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let mut rx = self.delegation_tx.subscribe();
        Ok(Response::new(Box::pin(stream! {
            while let Ok(update) = rx.recv().await {
                if let Some(delegation) = &update.delegation {
                    if Self::matches_delegation_filter(delegation, &filter) {
                        yield Ok(update);
                    }
                }
            }
        })))
    }
}
