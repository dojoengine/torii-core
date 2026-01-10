//! Retry policy for extractors
//!
//! Provides configurable retry logic for network requests and transient failures.

use anyhow::Result;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (0 = no retries, just try once)
    pub max_retries: u32,

    /// Initial backoff duration
    pub initial_backoff: Duration,

    /// Maximum backoff duration (prevents exponential growth from becoming too large)
    pub max_backoff: Duration,

    /// Backoff multiplier (e.g., 2.0 for exponential backoff)
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Creates a new retry policy with custom configuration.
    pub fn new(
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
        backoff_multiplier: f64,
    ) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
            backoff_multiplier,
        }
    }

    /// Creates a policy with no retries (fail immediately).
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            backoff_multiplier: 1.0,
        }
    }

    /// Creates a policy with aggressive retries (useful for transient network issues).
    pub fn aggressive() -> Self {
        Self {
            max_retries: 10,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 1.5,
        }
    }

    /// Executes a function with retry logic.
    ///
    /// # Arguments
    /// * `operation` - Async function to execute. Will be called multiple times on failure.
    ///
    /// # Returns
    /// Result from the operation, or the last error if all retries exhausted.
    ///
    /// # Example
    /// ```rust,ignore
    /// let policy = RetryPolicy::default();
    /// let result = policy.execute(|| async {
    ///     // Your network request here.
    ///     provider.get_block(block_number).await
    /// }).await?;
    /// ```
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        let mut backoff = self.initial_backoff;

        loop {
            match operation().await {
                Ok(result) => {
                    if attempts > 0 {
                        tracing::info!(
                            target: "torii::etl::retry",
                            "Operation succeeded after {} attempt(s)",
                            attempts + 1
                        );
                    }
                    return Ok(result);
                }
                Err(err) => {
                    attempts += 1;

                    if attempts > self.max_retries {
                        tracing::error!(
                            target: "torii::etl::retry",
                            "Operation failed after {} attempts: {:?}",
                            attempts,
                            err
                        );
                        return Err(err);
                    }

                    tracing::warn!(
                        target: "torii::etl::retry",
                        "Operation failed (attempt {}/{}): {:?}. Retrying in {:?}...",
                        attempts,
                        self.max_retries + 1,
                        err,
                        backoff
                    );

                    sleep(backoff).await;

                    // Calculate next backoff with exponential growth
                    backoff = Duration::from_secs_f64(
                        (backoff.as_secs_f64() * self.backoff_multiplier).min(self.max_backoff.as_secs_f64()),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let policy = RetryPolicy::default();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = policy
            .execute(|| {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(42)
                }
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only one attempt
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(50), 2.0);
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = policy
            .execute(|| {
                let c = counter_clone.clone();
                async move {
                    let count = c.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        anyhow::bail!("Simulated failure");
                    }
                    Ok::<_, anyhow::Error>(42)
                }
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        // 2 failures + 1 success
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let policy = RetryPolicy::new(2, Duration::from_millis(5), Duration::from_millis(20), 2.0);
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<(), anyhow::Error> = policy
            .execute(|| {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    anyhow::bail!("Always fails")
                }
            })
            .await;

        assert!(result.is_err());
        // Initial + 2 retries
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_no_retry_policy() {
        let policy = RetryPolicy::no_retry();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<(), anyhow::Error> = policy
            .execute(|| {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    anyhow::bail!("Fails immediately")
                }
            })
            .await;

        assert!(result.is_err());
        // Only one attempt
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
