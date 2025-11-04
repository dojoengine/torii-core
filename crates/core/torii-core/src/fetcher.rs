//! Abstractions for sourcing Starknet events.

use crate::types::{FetchOutcome, FetchPlan, FetcherCursor};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct FetchOptions {
    pub per_filter_event_limit: usize,
    pub max_concurrent_filters: usize,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            per_filter_event_limit: 2_560,
            max_concurrent_filters: 1,
        }
    }
}

/// Component responsible for retrieving raw Starknet events based on a [`FetchPlan`].
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Fetch a batch of events satisfying the provided plan.
    ///
    /// The optional `cursor` corresponds to the value returned in the previous invocation and
    /// allows the fetcher to resume pagination without keeping internal mutable state.
    async fn fetch(
        &self,
        plan: &FetchPlan,
        cursor: Option<&FetcherCursor>,
        options: &FetchOptions,
    ) -> Result<FetchOutcome>;
}
