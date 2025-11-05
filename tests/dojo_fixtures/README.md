# Dojo Fixtures

Generate and manage test fixtures for Torii from Dojo contracts.

## Overview

This CLI application provides two main capabilities for managing Dojo test fixtures:

### Local Fixture Generation (Migrate)
1. Deploys baseline and upgraded Cairo contracts to a local Katana node
2. Executes transactions to generate events with real data
3. Captures all events in a structured JSON format

The fixtures exercise schema migrations, model updates, and various Dojo world operations.

### Production Event Capture (Fetch)
1. Fetches events from deployed contracts on mainnet or testnet
2. Filters events by block range and event names
3. Saves events for testing with real-world data

## Project Structure

```
dojo_fixtures/
├── cairo/
│   ├── baseline/          # Original schema
│   └── upgrade/           # Upgraded schema with additional fields
├── src/
│   ├── main.rs            # CLI entry point
│   ├── abigen.rs          # Cainome-generated contract bindings
│   ├── constants.rs       # Shared constants
│   ├── deployment.rs      # Deployment info extraction
│   ├── events.rs          # Event fetching logic
│   ├── sozo.rs            # Sozo command wrapper
│   ├── transactions/      # Transaction sending logic
│   │   ├── baseline.rs    # Baseline contract calls
│   │   └── upgrade.rs     # Upgrade contract calls
│   └── types.rs           # Shared type definitions
└── build.rs               # Contract compilation
```

## Build Process

The `build.rs` script compiles Cairo contracts using `sozo`. Contract artifacts are then used by `cainome` to generate type-safe Rust bindings.

Set `DOJO_FIXTURES_BUILD_CONTRACTS=1` to force rebuild:

```bash
DOJO_FIXTURES_BUILD_CONTRACTS=1 cargo build
```

## CLI Usage

The CLI provides two main commands: `migrate` for local testing and `fetch` for retrieving events from deployed contracts.

### Migrate Command

Deploy baseline and upgrade contracts to a local Katana node, execute transactions, and capture events:

```bash
# Migrate with default settings
cargo run migrate --katana-db-dir ./katana_db

# Specify custom output file and contract tag
cargo run migrate --katana-db-dir ./katana_db --output-events ./my_events.json --contract-tag ns-c1
```

**Options:**
- `--katana-db-dir` (required) - Directory for Katana database
- `--output-events` (optional) - Output file path (default: `dojo_fixtures_events.json`)
- `--contract-tag` (optional) - Contract tag to fetch events for (default: `ns-c1`)

### Fetch Command

Fetch events from a deployed contract on mainnet or testnet:

```bash
# Fetch all events from a contract on mainnet
cargo run fetch \
  --contract-address 0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5 \
  --rpc-url https://api.cartridge.gg/x/starknet/mainnet \
  --output-events /tmp/pistols.json \
  --from-block 1376383

# Fetch specific events by name with block range
cargo run fetch \
  --contract-address 0x1234... \
  --rpc-url https://starknet-sepolia.public.blastapi.io \
  --output-events ./events.json \
  --from-block 100000 \
  --to-block 200000 \
  --event-names StoreSetRecord --event-names StoreUpdateRecord
```

**Options:**
- `--contract-address` (required) - Contract address to fetch events from
- `--rpc-url` (required) - RPC endpoint URL
- `--output-events` (required) - Output file path
- `--from-block` (optional) - Starting block number (default: 0)
- `--to-block` (optional) - Ending block number (default: latest)
- `--event-names` (optional) - Filter by specific event names (can be specified multiple times)

### Output Formats

#### Migrate Command Output

The migrate command generates a JSON file containing the world address and all captured events:

```json
{
  "world_address": "0x079b90aa209a333d5c4b2621d121adeecdb7d705c81615a67944d4fb233c9666",
  "events": [
    {
      "from_address": "0x...",
      "keys": ["0x..."],
      "data": ["0x..."],
      "block_hash": "0x...",
      "block_number": 2,
      "transaction_hash": "0x..."
    }
  ]
}
```

This fixture can be used for:
- Testing Torii's event processing without a live Katana node
- Regression testing of schema migrations
- Validating event parsing and model updates

#### Fetch Command Output

The fetch command generates a JSON file containing only the events array:

```json
[
  {
    "from_address": "0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5",
    "keys": ["0x..."],
    "data": ["0x..."],
    "block_hash": "0x...",
    "block_number": 1376383,
    "transaction_hash": "0x..."
  }
]
```

This is useful for:
- Capturing real-world events from deployed contracts
- Testing with production data
- Analyzing event patterns and data structures

## Transaction Coverage

### Baseline Contract

The baseline deployment exercises these functions:
- `write_model` - Single model write
- `write_models` - Batch model writes
- `write_member_model` - Write specific model member (e field)
- `write_member_of_models` - Batch member writes
- `write_model_legacy` - Legacy model write
- `write_models_legacy` - Batch legacy model writes
- `write_member_model_legacy` - Legacy member write
- `write_member_of_models_legacy` - Batch legacy member writes

### Upgrade Contract

The upgrade deployment includes all baseline functions plus:
- `write_member_score` - Write new score field
- `write_member_score_legacy` - Write score field (legacy storage)

All calls include the new `score` field to exercise schema migration.

## Technical Details

### Cainome Integration

Contract bindings are generated using `cainome::rs::abigen!` from compiled contract artifacts. This provides:
- Type-safe contract interactions
- Automatic calldata serialization
- Compile-time validation of contract calls

### Katana Integration

Uses `katana-runner` to:
- Spawn a local Katana instance with fresh state
- Configure pre-funded accounts
- Manage lifecycle (cleanup on exit)

### Event Collection

Fetches all events from the world contract using paginated RPC calls, ensuring complete coverage of generated fixtures.

## Development

### Requirements

- `sozo` - Dojo CLI tool for building and deploying contracts
- Rust toolchain (1.70+)

### Making Changes

1. **Modify Cairo contracts**: Edit files in `cairo/baseline/` or `cairo/upgrade/`
2. **Rebuild contracts**: Run with `DOJO_FIXTURES_BUILD_CONTRACTS=1`
3. **Update transactions**: Modify `src/transactions/{baseline,upgrade}.rs`
4. **Regenerate fixtures**: Run `cargo run --bin dojo-fixtures -- migrate`

### Adding New Functions

1. Add the function to the Cairo contract
2. Run `DOJO_FIXTURES_BUILD_CONTRACTS=1 cargo build`
3. Add a call in the appropriate `src/transactions/*.rs` file using the cainome bindings
4. Regenerate fixtures

The cainome bindings make adding new calls straightforward:

```rust
let model = Model {
    player: player.into(),
    e: Enum1::Left,
    index: 42,
};
let tx_result = contract.write_model(&model).send().await?;
```
