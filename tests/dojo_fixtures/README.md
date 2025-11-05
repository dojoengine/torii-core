# Dojo Fixtures

Generate and manage test fixtures for Torii from Dojo contracts.

## Overview

This CLI application creates comprehensive test fixtures for Torii by:
1. Deploying baseline and upgraded Cairo contracts to a Katana node
2. Executing transactions to generate events with real data
3. Capturing all events in a structured JSON format

The fixtures exercise schema migrations, model updates, and various Dojo world operations.

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

### Migrate Command

Deploy baseline and upgrade contracts to Katana, execute transactions, and capture events:

```bash
# Use default settings (starts Katana, saves to dojo_fixtures_events.json)
cargo run --bin dojo-fixtures -- migrate

# Specify custom output file
cargo run --bin dojo-fixtures -- migrate --output-events ./my_events.json

# Specify Katana database directory
cargo run --bin dojo-fixtures -- migrate --katana-db-dir ./my_katana_db
```

### Output Format

The migrate command generates a JSON file with this structure:

```json
{
  "world_address": "0x...",
  "events": [
    {
      "from_address": "0x...",
      "keys": ["0x..."],
      "data": ["0x..."],
      "block_hash": "0x...",
      "block_number": 123,
      "transaction_hash": "0x..."
    }
  ]
}
```

This fixture can be used for:
- Testing Torii's event processing without a live Katana node
- Regression testing of schema migrations
- Validating event parsing and model updates

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
