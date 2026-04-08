pub mod decoder;
pub mod discovery;
pub mod grpc_service;
pub mod identification;
pub mod sink;
pub mod storage;

pub mod proto {
    include!("generated/torii.sinks.governance.rs");
}

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/governance_descriptor.bin");

pub use decoder::{
    DelegateChanged, DelegateVotesChanged, GovernanceDecoder, ProposalCanceled, ProposalCreated,
    ProposalExecuted, ProposalQueued, VoteCast, VoteCastWithParams,
};
pub use discovery::{
    bootstrap_governance_registry, fetch_current_votes, fetch_proposal_metadata,
    governance_decoder_id, register_governor_and_votes_token, GovernanceBootstrap,
    GovernanceMetadata, ProposalMetadata,
};
pub use grpc_service::GovernanceService;
pub use identification::GovernanceRule;
pub use sink::GovernanceSink;
pub use storage::{
    DelegationCursor, DelegationRecord, GovernanceStorage, GovernorRecord, ProposalCursor,
    ProposalRecord, VoteCursor, VoteRecord, VotingPowerRecord, VotingPowerSource,
};
