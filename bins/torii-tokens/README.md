# Torii Tokens

Unified Starknet token indexer for ERC20, ERC721, and ERC1155 tokens.

## Quick Start

```bash
# Block-range mode: auto-discovers all token contracts
torii-tokens --from-block 100000

# Event mode: index specific contracts only
torii-tokens --mode event --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7 --from-block 100000
```

## Features

- **Multi-token support**: ERC20, ERC721, and ERC1155 in a single indexer
- **Balance tracking**: Real-time balance computation with on-chain verification
- **Auto-discovery**: Automatically identifies token contracts by ABI inspection (block-range mode)
- **gRPC subscriptions**: Real-time streaming of token events
- **Historical queries**: Paginated queries for transfers, approvals, and ownership
- **Flexible extraction**: Block-range mode for full indexing, event mode for targeted contracts

## Installation

```bash
# From the repository root
cargo build --release -p torii-tokens

# Binary will be at target/release/torii-tokens
```

## Usage

### Block Range Mode (default)

Fetches ALL events from each block. Best for full chain indexing with auto-discovery.

```bash
# Auto-discover all token contracts from block 100000
torii-tokens --from-block 100000

# Index with explicit contracts (will also auto-discover others)
torii-tokens --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7 --from-block 0

# Index multiple token types
torii-tokens \
  --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7 \
  --erc721 0x...nft \
  --erc1155 0x...game_items \
  --from-block 0
```

### Event Mode

Uses `starknet_getEvents` with per-contract cursors. Best for indexing specific contracts.

```bash
# Index specific ERC20 contracts
torii-tokens --mode event \
  --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7 \
  --from-block 0

# Index multiple contracts
torii-tokens --mode event \
  --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7,0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D \
  --from-block 0
```

### Custom Configuration

```bash
# Custom RPC and port
torii-tokens \
  --rpc-url https://your-rpc.example.com \
  --port 8080 \
  --from-block 0

# Custom database directory
torii-tokens --db-dir /path/to/data --from-block 0
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `block-range` | Extraction mode (`block-range` or `event`) |
| `--rpc-url` | Cartridge mainnet | Starknet RPC endpoint |
| `--from-block` | `0` | Starting block number |
| `--to-block` | None | Ending block (None = follow chain head) |
| `--db-dir` | `./torii-data` | Directory for database files |
| `--database-url` | None | Engine DB URL/path (e.g. `postgres://...`) |
| `--port` | `3000` | HTTP/gRPC server port |
| `--erc20` | None | ERC20 contract addresses (comma-separated) |
| `--erc721` | None | ERC721 contract addresses (comma-separated) |
| `--erc1155` | None | ERC1155 contract addresses (comma-separated) |
| `--batch-size` | `1000` | Blocks per batch (block-range mode) |
| `--event-chunk-size` | `1000` | Events per RPC request (event mode) |
| `--event-block-batch-size` | `10000` | Block range per iteration (event mode) |

## Extraction Modes

### Block Range Mode

- Fetches complete block data in batches
- Inspects contract ABIs to identify token types automatically
- Single cursor tracks overall progress
- Best for full chain indexing

### Event Mode

- Each contract has its own cursor
- Adding a new contract starts fresh from `--from-block`
- Existing contracts resume from their saved position
- Best for targeted indexing with lower resource usage

**Adding contracts in event mode:**
```bash
# Initial run
torii-tokens --mode event --erc20 0x...ETH,0x...STRK --from-block 0

# Add USDC later - ETH and STRK resume from their cursors, USDC starts from block 0
torii-tokens --mode event --erc20 0x...ETH,0x...STRK,0x...USDC --from-block 0
```

## Database and Cursors

The indexer maintains internal cursors to track indexing progress. On restart, indexing resumes from the last processed position.

**To reset and re-index from scratch, delete the database directory:**

```bash
rm -rf ./torii-data
torii-tokens --from-block 0
```

The indexer creates separate SQLite databases for each component:

```

Set `--database-url` (or `DATABASE_URL`) to run engine + token storages on PostgreSQL. If unset, the local SQLite files below are used.
./torii-data/
  engine.db     # ETL state, cursors, statistics
  erc20.db      # ERC20 transfers, approvals, balances
  erc721.db     # ERC721 transfers, ownership
  erc1155.db    # ERC1155 transfers, balances
```

Each database uses WAL mode for performance and crash safety.

## gRPC API Reference

### Available Services

```bash
# List all services
grpcurl -plaintext localhost:3000 list

# Services:
# - grpc.reflection.v1.ServerReflection
# - torii.Torii (EventBus subscriptions)
# - torii.sinks.erc20.Erc20
# - torii.sinks.erc721.Erc721
# - torii.sinks.erc1155.Erc1155
```

### Address Encoding

All addresses and U256 values are encoded as **base64-encoded big-endian bytes**.

```bash
# Convert hex address to base64
printf '%s' "049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7" | xxd -r -p | base64
# Output: BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=

# Convert base64 back to hex
echo "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=" | base64 -d | xxd -p
```

---

### ERC20 Service

**Service:** `torii.sinks.erc20.Erc20`

#### GetStats

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc20.Erc20/GetStats
```

#### GetTransfers

```bash
# Get all transfers (first 100)
grpcurl -plaintext -d '{}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by wallet (matches from OR to)
grpcurl -plaintext -d '{
  "filter": {"wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="},
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by token contract
grpcurl -plaintext -d '{
  "filter": {"tokens": ["BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="]},
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by block range
grpcurl -plaintext -d '{
  "filter": {"blockFrom": "100000", "blockTo": "200000"},
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Pagination
grpcurl -plaintext -d '{
  "cursor": {"blockNumber": "150000", "id": "42"},
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers
```

#### GetApprovals

```bash
# Filter by account (matches owner OR spender)
grpcurl -plaintext -d '{
  "filter": {"account": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="},
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetApprovals
```

#### SubscribeTransfers

```bash
# Subscribe to all transfers
grpcurl -plaintext -d '{"clientId": "my-client"}' \
  localhost:3000 torii.sinks.erc20.Erc20/SubscribeTransfers

# Subscribe with wallet filter
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {"wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="}
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeTransfers
```

#### SubscribeApprovals

```bash
grpcurl -plaintext -d '{"clientId": "my-client"}' \
  localhost:3000 torii.sinks.erc20.Erc20/SubscribeApprovals
```

**ERC20 Filter Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `tokens` | bytes[] | Token contract whitelist |
| `direction` | enum | `DIRECTION_ALL`, `DIRECTION_SENT`, `DIRECTION_RECEIVED` |
| `blockFrom` | uint64 | Minimum block number |
| `blockTo` | uint64 | Maximum block number |

---

### ERC721 Service

**Service:** `torii.sinks.erc721.Erc721`

#### GetStats

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc721.Erc721/GetStats
```

#### GetTransfers

```bash
# Filter by wallet
grpcurl -plaintext -d '{
  "filter": {"wallet": "...base64..."},
  "limit": 10
}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers

# Filter by specific NFT token IDs
grpcurl -plaintext -d '{
  "filter": {"tokenIds": ["AQ==", "Ag==", "Aw=="]},
  "limit": 10
}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers
```

#### GetOwnership

```bash
# Get all NFTs owned by an address
grpcurl -plaintext -d '{
  "filter": {"owner": "...base64..."},
  "limit": 100
}' localhost:3000 torii.sinks.erc721.Erc721/GetOwnership
```

#### GetOwner

```bash
# Get owner of a specific NFT
grpcurl -plaintext -d '{
  "token": "...nft_contract_base64...",
  "tokenId": "AQ=="
}' localhost:3000 torii.sinks.erc721.Erc721/GetOwner
```

#### SubscribeTransfers

```bash
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {"tokenIds": ["AQ==", "Ag=="]}
}' localhost:3000 torii.sinks.erc721.Erc721/SubscribeTransfers
```

**ERC721 Filter Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `tokens` | bytes[] | NFT contract whitelist |
| `tokenIds` | bytes[] | Specific NFT token IDs |
| `blockFrom` | uint64 | Minimum block number |
| `blockTo` | uint64 | Maximum block number |

---

### ERC1155 Service

**Service:** `torii.sinks.erc1155.Erc1155`

#### GetStats

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc1155.Erc1155/GetStats
```

#### GetTransfers

```bash
# Filter by wallet
grpcurl -plaintext -d '{
  "filter": {"wallet": "...base64..."},
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by operator
grpcurl -plaintext -d '{
  "filter": {"operator": "...base64..."},
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by specific token IDs
grpcurl -plaintext -d '{
  "filter": {"tokenIds": ["AQ==", "Ag=="]},
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers
```

#### SubscribeTransfers

```bash
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {"tokens": ["...game_items_contract..."]}
}' localhost:3000 torii.sinks.erc1155.Erc1155/SubscribeTransfers
```

**ERC1155 Filter Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `operator` | bytes | Exact operator address |
| `tokens` | bytes[] | Token contract whitelist |
| `tokenIds` | bytes[] | Specific token IDs |
| `blockFrom` | uint64 | Minimum block number |
| `blockTo` | uint64 | Maximum block number |

---

### Core Torii Service

**Service:** `torii.Torii`

EventBus-based subscriptions that work across all sinks.

#### ListTopics

```bash
grpcurl -plaintext localhost:3000 torii.Torii/ListTopics
```

#### SubscribeToTopicsStream

```bash
# Subscribe to ERC20 transfers via EventBus
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "topics": [{"topic": "erc20.transfer"}]
}' localhost:3000 torii.Torii/SubscribeToTopicsStream
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `STARKNET_RPC_URL` | Default RPC URL (overridden by `--rpc-url`) |
| `RUST_LOG` | Log level (e.g., `info`, `debug`, `torii=debug`) |

## Logging

Control log verbosity with `RUST_LOG`:

```bash
# Default info level
torii-tokens --from-block 0

# Debug logging for torii components
RUST_LOG=torii=debug torii-tokens --from-block 0

# Trace all SQL queries
RUST_LOG=torii=debug,sqlx=trace torii-tokens --from-block 0
```
