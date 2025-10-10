//! Abstractions for sourcing Starknet events.

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::EmittedEvent;

use crate::types::FetchPlan;

/// Component responsible for retrieving raw Starknet events based on a [`FetchPlan`].
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Fetch a batch of events satisfying the provided plan.
    async fn fetch(&self, plan: &FetchPlan) -> Result<Vec<EmittedEvent>>;
}
