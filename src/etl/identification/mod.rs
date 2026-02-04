//! Contract identification system for auto-discovery.
//!
//! This module provides automatic contract identification by inspecting ABIs.
//! When encountering events from unknown contracts, the system can:
//! 1. Fetch the contract's ABI from the chain
//! 2. Run pluggable identification rules (ERC20, ERC721, etc.)
//! 3. Cache the contract→decoder mapping for future events
//!
//! # Architecture
//!
//! ```text
//! ExtractionBatch → ContractRegistry → DecoderContext → Sinks
//!                        ↓
//!                   EngineDb (cache)
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use torii::etl::identification::{ContractRegistry, IdentificationRule};
//!
//! // Register identification rules
//! let registry = ContractRegistry::new(provider, engine_db)
//!     .with_rule(Box::new(Erc20Rule::new()))
//!     .with_rule(Box::new(Erc721Rule::new()));
//!
//! // Load cached mappings from database
//! registry.load_from_db().await?;
//!
//! // Pass cache to config
//! let config = ToriiConfig::builder()
//!     .with_registry_cache(registry.shared_cache())
//!     .build();
//! ```

mod registry;
mod rule;

pub use registry::{ContractIdentifier, ContractRegistry};
pub use rule::IdentificationRule;
