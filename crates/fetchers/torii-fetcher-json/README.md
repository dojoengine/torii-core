# Torii JSON Fetcher

A file-based fetcher implementation for Torii that reads events from JSON files. This is particularly useful for testing and replaying captured events without requiring a live RPC connection.

## Features

- **Flexible JSON Format Support**: Automatically detects and parses two JSON formats:
  - Direct array: `[{...}, {...}]`
  - Object with metadata: `{"world_address": "0x...", "events": [{...}]}`
- **Pagination Simulation**: Uses cursor-based indexing to simulate chunked event fetching
- **Event Filtering**: Supports filtering by contract address and event selectors
- **In-Memory Caching**: Events are loaded once and served from memory

## Usage

### Configuration

```toml
[fetcher]
type = "json"
file_path = "./path/to/events.json"
chunk_size = 100  # Optional: override per_filter_event_limit
```

### JSON File Formats

#### Format 1: Direct Array

```json
[
  {
    "from_address": "0x...",
    "keys": ["0x..."],
    "data": ["0x..."],
    "block_hash": "0x...",
    "block_number": 123,
    "transaction_hash": "0x..."
  }
]
```

#### Format 2: With Metadata (Dojo Fixtures Format)

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

### Creating Test Fixtures

Use the `dojo-fixtures` CLI to generate event files:

```bash
# Generate events from local Katana
cd tests/dojo_fixtures
cargo run migrate --katana-db-dir ./katana_db --output-events events.json

# Fetch events from deployed contracts
cargo run fetch \
  --contract-address 0x... \
  --rpc-url https://api.cartridge.gg/x/starknet/mainnet \
  --output-events events.json \
  --from-block 1000000
```

## How It Works

1. **Loading**: The fetcher loads all events from the JSON file at initialization and groups them by contract address.

2. **Pagination**: The cursor stores the current index for each contract address as a string. On each fetch:
   - The fetcher retrieves the current index from the cursor
   - Returns up to `chunk_size` events starting from that index
   - Updates the cursor with the new index
   - Sets continuation to `None` when all events are consumed

3. **Filtering**: Events are filtered based on:
   - Contract address (from `FetchPlan.address_selectors`)
   - Event selectors (if specified in `ContractFilter.selectors`)

## Example

```rust
use torii_fetcher_json::{JsonFetcher, JsonFetcherConfig};

let config = JsonFetcherConfig {
    file_path: "./events.json".to_string(),
    chunk_size: Some(100),
};

let fetcher = JsonFetcher::new(config)?;
```

## Testing

Run the included tests:

```bash
cargo test -p torii-fetcher-json
```

## Use Cases

- **Unit Testing**: Test event processing logic without a live blockchain
- **Integration Testing**: Replay captured events for regression testing
- **Development**: Work offline with pre-captured event data
- **Debugging**: Isolate and replay specific event sequences

