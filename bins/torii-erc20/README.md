# Torii ERC20 - Starknet ERC20 Token Indexer

A production-ready indexer for ERC20 tokens on Starknet.

## Features

- ✅ **Explicit Contract Mapping**: Pre-configure known tokens (ETH, STRK, custom)
- ✅ **Auto-Discovery**: Automatically detect and index new ERC20 contracts
- ✅ **Strict Mode**: Disable auto-discovery for controlled production deployments
- ✅ **Real-time Balance Tracking**: Maintain up-to-date balances for all addresses
- ✅ **Transfer History**: Complete audit trail of all ERC20 transfers
- ✅ **SQLite Storage**: Efficient local database with full history
- ✅ **gRPC Subscriptions**: Real-time updates via Torii's event bus

## Installation

```bash
# From workspace root
cargo build --release --bin torii-erc20

# Binary will be at:
# target/release/torii-erc20
```

## Usage

### Basic Usage (Auto-Discovery Enabled)

```bash
# Start indexing from block 100,000
torii-erc20 --from-block 100000
```

This will:
- Index ETH and STRK tokens (explicit mappings)
- Auto-discover other ERC20 contracts via ABI heuristics
- Store data in `./erc20-data.db`
- Start gRPC server on port 3000

### Strict Mode (Production)

```bash
# Only index explicitly configured contracts
torii-erc20 --no-auto-discovery
```

This mode:
- **Disables** auto-discovery
- Only indexes ETH and STRK
- Useful for production where you want strict control

### Custom Contracts

```bash
# Index specific contracts only
torii-erc20 \
  --no-auto-discovery \
  --contracts 0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7,0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d
```

### Network Selection

```bash
# Mainnet
torii-erc20 --rpc-url https://starknet-mainnet.public.blastapi.io/rpc/v0_7

# Sepolia (default)
torii-erc20 --rpc-url https://starknet-sepolia.public.blastapi.io/rpc/v0_7
```

### All Options

```bash
torii-erc20 \
  --rpc-url <RPC_URL> \
  --from-block <START> \
  --to-block <END> \
  --db-path ./my-data.db \
  --port 3000 \
  --no-auto-discovery \
  --contracts <ADDR1>,<ADDR2>
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Torii ERC20 Binary                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌────────────────────────────────┐  │
│  │ BlockRange   │─────▶│  ContractRegistry              │  │
│  │ Extractor    │      │  - ETH (explicit)              │  │
│  └──────────────┘      │  - STRK (explicit)             │  │
│         │              │  - ERC20Rule (auto-discovery)  │  │
│         │              └────────────────────────────────┘  │
│         │                           │                        │
│         ▼                           ▼                        │
│  ┌──────────────┐      ┌────────────────────────────────┐  │
│  │   Events     │─────▶│  MultiDecoder + Registry       │  │
│  │              │      │  (lazy identification)         │  │
│  └──────────────┘      └────────────────────────────────┘  │
│                                    │                         │
│                                    ▼                         │
│                        ┌────────────────────────────────┐  │
│                        │  ERC20 Decoder                 │  │
│                        │  (Transfer events)             │  │
│                        └────────────────────────────────┘  │
│                                    │                         │
│                                    ▼                         │
│                        ┌────────────────────────────────┐  │
│                        │  ERC20 Sink                    │  │
│                        │  - Store transfers             │  │
│                        │  - Update balances             │  │
│                        └────────────────────────────────┘  │
│                                    │                         │
│                                    ▼                         │
│                        ┌────────────────────────────────┐  │
│                        │  SQLite Storage                │  │
│                        │  - transfers table             │  │
│                        │  - balances table              │  │
│                        └────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Database Schema

### Transfers Table

```sql
CREATE TABLE transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL,
    from_addr TEXT NOT NULL,
    to_addr TEXT NOT NULL,
    amount TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    timestamp INTEGER DEFAULT (strftime('%s', 'now')),
    UNIQUE(token, tx_hash, from_addr, to_addr)
);
```

### Balances Table

```sql
CREATE TABLE balances (
    token TEXT NOT NULL,
    address TEXT NOT NULL,
    balance TEXT NOT NULL,
    updated_at INTEGER DEFAULT (strftime('%s', 'now')),
    PRIMARY KEY (token, address)
);
```

## Contract Identification

### Explicit Mapping (Highest Priority)

Well-known contracts are explicitly mapped:
- **Mainnet ETH**: `0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7`
- **Mainnet STRK**: `0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d`
- **Sepolia ETH**: Same as mainnet
- **Sepolia STRK**: Same as mainnet

### Auto-Discovery (When Enabled)

The ERC20Rule automatically identifies contracts with:
- `transfer()`, `balance_of()`, `total_supply()` functions
- `Transfer` event signature

### Performance

**Lazy Identification:**
- Contracts identified on first event (network call)
- Subsequent events from same contract use cached result
- No pre-iteration of events needed
- Lock-per-event approach (see performance notes in code for optimization path)

## Example Queries

```bash
# Connect to database
sqlite3 erc20-data.db

# Get total transfers
SELECT COUNT(*) FROM transfers;

# Get transfers for ETH
SELECT * FROM transfers
WHERE token = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
LIMIT 10;

# Get top 10 addresses by balance for a token
SELECT address, balance
FROM balances
WHERE token = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
ORDER BY CAST(balance AS INTEGER) DESC
LIMIT 10;

# Get unique tokens indexed
SELECT COUNT(DISTINCT token) FROM transfers;
```

## Development

### Running Tests

```bash
cargo test --bin torii-erc20
```

### Adding Custom Tokens

Edit `src/config.rs` to add more well-known contracts:

```rust
pub fn well_known_contracts(&self) -> Vec<(Felt, &'static str)> {
    vec![
        (Felt::from_hex_unchecked("0x..."), "MY_TOKEN"),
        // ...
    ]
}
```

## Performance Considerations

- **Lock overhead**: Currently locks mutex per event. See code comments for optimization path if this becomes a bottleneck.
- **Balance calculation**: Current implementation uses simple string storage. Production should use proper u256 arithmetic.
- **Database**: SQLite performs well for moderate loads. For high-throughput production, consider PostgreSQL.

## Performance Profiling

The binary includes built-in timing instrumentation that measures each phase of the ETL loop:
- Extract time (RPC calls)
- Decode time (includes contract identification)
- Sink processing time (database writes)
- Total loop time

Example output:
```
📦 Batch #1: Extracted 1234 events from 10 blocks (extract_time: 450.23ms)
   ✓ Decoded into 156 envelopes (decode_time: 89.45ms)
   ✓ Processed through sink (sink_time: 23.12ms) | Total loop: 562.80ms
```

For detailed CPU profiling with flamegraphs, see **[PROFILING.md](PROFILING.md)**.

## Future Improvements

- [ ] Proper u256 balance arithmetic
- [ ] Token metadata (name, symbol, decimals)
- [ ] HTTP API for querying balances
- [ ] WebSocket subscriptions for real-time updates
- [ ] PostgreSQL support
- [ ] Dashboard UI

## License

MIT
