# Token gRPC API Reference

This document describes the gRPC queries and subscriptions available for each token type (ERC20, ERC721, ERC1155).

## Quick Start

```bash
# Start the unified indexer
torii-tokens --include-well-known --from-block 100000

# List all available services
grpcurl -plaintext localhost:3000 list

# Expected output:
# grpc.reflection.v1.ServerReflection
# grpc.reflection.v1alpha.ServerReflection
# torii.Torii
# torii.sinks.erc20.Erc20
# torii.sinks.erc721.Erc721
# torii.sinks.erc1155.Erc1155
```

## Address Encoding

All addresses and U256 values are encoded as **base64-encoded big-endian bytes**.

To convert a hex address to base64:
```bash
# Example: ETH contract address
# 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7

# Using printf and base64:
printf '%s' "049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7" | xxd -r -p | base64
# Output: BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=
```

---

## ERC20 Service

**Service:** `torii.sinks.erc20.Erc20`

### GetStats

Get indexer statistics.

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc20.Erc20/GetStats
```

**Response:**
```json
{
  "totalTransfers": "12345",
  "totalApprovals": "678",
  "uniqueTokens": "3",
  "latestBlock": "750000"
}
```

### GetTransfers

Query historical transfers with filtering and pagination.

```bash
# Get all transfers (first 100)
grpcurl -plaintext -d '{}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Get transfers with limit
grpcurl -plaintext -d '{"limit": 10}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by wallet (matches from OR to)
grpcurl -plaintext -d '{
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by exact sender
grpcurl -plaintext -d '{
  "filter": {
    "from": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by token contract
grpcurl -plaintext -d '{
  "filter": {
    "tokens": ["BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="]
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Filter by block range
grpcurl -plaintext -d '{
  "filter": {
    "blockFrom": "100000",
    "blockTo": "200000"
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers

# Pagination (use next_cursor from previous response)
grpcurl -plaintext -d '{
  "cursor": {
    "blockNumber": "150000",
    "id": "42"
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetTransfers
```

**Response:**
```json
{
  "transfers": [
    {
      "token": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=",
      "from": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
      "to": "BHGPWgPDTMGvFqHe6Y/7IMMe9c1h2rByAYWPQoe8k40=",
      "amount": "DeC2s6dkAAA=",
      "blockNumber": "123456",
      "txHash": "...",
      "timestamp": "1699123456"
    }
  ],
  "nextCursor": {
    "blockNumber": "123456",
    "id": "100"
  }
}
```

### GetApprovals

Query historical approvals with filtering and pagination.

```bash
# Get all approvals
grpcurl -plaintext -d '{"limit": 10}' localhost:3000 torii.sinks.erc20.Erc20/GetApprovals

# Filter by account (matches owner OR spender)
grpcurl -plaintext -d '{
  "filter": {
    "account": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetApprovals

# Filter by exact owner
grpcurl -plaintext -d '{
  "filter": {
    "owner": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc20.Erc20/GetApprovals
```

### SubscribeTransfers

Subscribe to real-time transfer events.

```bash
# Subscribe to all transfers
grpcurl -plaintext -d '{
  "clientId": "my-client"
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeTransfers

# Subscribe with wallet filter
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  }
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeTransfers

# Subscribe to specific tokens
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "tokens": ["BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="]
  }
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeTransfers
```

### SubscribeApprovals

Subscribe to real-time approval events.

```bash
# Subscribe to all approvals
grpcurl -plaintext -d '{
  "clientId": "my-client"
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeApprovals

# Subscribe with owner filter
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "owner": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  }
}' localhost:3000 torii.sinks.erc20.Erc20/SubscribeApprovals
```

---

## ERC721 Service

**Service:** `torii.sinks.erc721.Erc721`

### GetStats

Get indexer statistics.

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc721.Erc721/GetStats
```

**Response:**
```json
{
  "totalTransfers": "5000",
  "uniqueTokens": "10",
  "uniqueNfts": "2500",
  "latestBlock": "750000"
}
```

### GetTransfers

Query historical NFT transfers.

```bash
# Get all transfers
grpcurl -plaintext -d '{"limit": 10}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers

# Filter by wallet (from OR to)
grpcurl -plaintext -d '{
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers

# Filter by specific NFT token IDs
grpcurl -plaintext -d '{
  "filter": {
    "tokenIds": ["AQ==", "Ag==", "Aw=="]
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers

# Filter by token contract and block range
grpcurl -plaintext -d '{
  "filter": {
    "tokens": ["...base64..."],
    "blockFrom": "100000",
    "blockTo": "200000"
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc721.Erc721/GetTransfers
```

**Response:**
```json
{
  "transfers": [
    {
      "token": "...",
      "tokenId": "AQ==",
      "from": "...",
      "to": "...",
      "blockNumber": "123456",
      "txHash": "...",
      "timestamp": "1699123456"
    }
  ],
  "nextCursor": {
    "blockNumber": "123456",
    "id": "50"
  }
}
```

### GetOwnership

Query current NFT ownership.

```bash
# Get all NFTs owned by an address
grpcurl -plaintext -d '{
  "filter": {
    "owner": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 100
}' localhost:3000 torii.sinks.erc721.Erc721/GetOwnership

# Get ownership for specific token contracts
grpcurl -plaintext -d '{
  "filter": {
    "owner": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=",
    "tokens": ["...nft_contract_base64..."]
  },
  "limit": 100
}' localhost:3000 torii.sinks.erc721.Erc721/GetOwnership
```

**Response:**
```json
{
  "ownership": [
    {
      "token": "...",
      "tokenId": "AQ==",
      "owner": "...",
      "blockNumber": "123456"
    }
  ],
  "nextCursor": null
}
```

### GetOwner

Get the current owner of a specific NFT.

```bash
grpcurl -plaintext -d '{
  "token": "...nft_contract_base64...",
  "tokenId": "AQ=="
}' localhost:3000 torii.sinks.erc721.Erc721/GetOwner
```

**Response:**
```json
{
  "owner": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
}
```

### SubscribeTransfers

Subscribe to real-time NFT transfer events.

```bash
# Subscribe to all NFT transfers
grpcurl -plaintext -d '{
  "clientId": "my-client"
}' localhost:3000 torii.sinks.erc721.Erc721/SubscribeTransfers

# Subscribe to transfers for a specific wallet
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  }
}' localhost:3000 torii.sinks.erc721.Erc721/SubscribeTransfers

# Subscribe to specific NFT IDs
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "tokenIds": ["AQ==", "Ag=="]
  }
}' localhost:3000 torii.sinks.erc721.Erc721/SubscribeTransfers
```

---

## ERC1155 Service

**Service:** `torii.sinks.erc1155.Erc1155`

### GetStats

Get indexer statistics.

```bash
grpcurl -plaintext localhost:3000 torii.sinks.erc1155.Erc1155/GetStats
```

**Response:**
```json
{
  "totalTransfers": "8000",
  "uniqueTokens": "5",
  "uniqueTokenIds": "150",
  "latestBlock": "750000"
}
```

### GetTransfers

Query historical token transfers (includes both single and batch transfers).

```bash
# Get all transfers
grpcurl -plaintext -d '{"limit": 10}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by wallet (from OR to)
grpcurl -plaintext -d '{
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by operator
grpcurl -plaintext -d '{
  "filter": {
    "operator": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by specific token IDs
grpcurl -plaintext -d '{
  "filter": {
    "tokenIds": ["AQ==", "Ag==", "Aw=="]
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers

# Filter by token contract and block range
grpcurl -plaintext -d '{
  "filter": {
    "tokens": ["...base64..."],
    "blockFrom": "100000",
    "blockTo": "200000"
  },
  "limit": 10
}' localhost:3000 torii.sinks.erc1155.Erc1155/GetTransfers
```

**Response:**
```json
{
  "transfers": [
    {
      "token": "...",
      "operator": "...",
      "from": "...",
      "to": "...",
      "tokenId": "AQ==",
      "amount": "Cg==",
      "blockNumber": "123456",
      "txHash": "...",
      "timestamp": "1699123456",
      "isBatch": false,
      "batchIndex": 0
    },
    {
      "token": "...",
      "operator": "...",
      "from": "...",
      "to": "...",
      "tokenId": "Ag==",
      "amount": "FA==",
      "blockNumber": "123457",
      "txHash": "...",
      "timestamp": "1699123500",
      "isBatch": true,
      "batchIndex": 0
    }
  ],
  "nextCursor": {
    "blockNumber": "123457",
    "id": "75"
  }
}
```

### SubscribeTransfers

Subscribe to real-time token transfer events.

```bash
# Subscribe to all transfers
grpcurl -plaintext -d '{
  "clientId": "my-client"
}' localhost:3000 torii.sinks.erc1155.Erc1155/SubscribeTransfers

# Subscribe with wallet filter
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "wallet": "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc="
  }
}' localhost:3000 torii.sinks.erc1155.Erc1155/SubscribeTransfers

# Subscribe to specific token IDs
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "tokenIds": ["AQ==", "Ag=="]
  }
}' localhost:3000 torii.sinks.erc1155.Erc1155/SubscribeTransfers

# Subscribe to specific contracts
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "filter": {
    "tokens": ["...game_items_contract..."]
  }
}' localhost:3000 torii.sinks.erc1155.Erc1155/SubscribeTransfers
```

---

## Core Torii Service

**Service:** `torii.Torii`

The core Torii service provides EventBus-based subscriptions that work across all sinks.

### ListTopics

List all available subscription topics.

```bash
grpcurl -plaintext localhost:3000 torii.Torii/ListTopics
```

### SubscribeToTopicsStream

Subscribe to EventBus topics (server-side streaming).

```bash
# Subscribe to ERC20 transfers via EventBus
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "topics": [
    {
      "topic": "erc20.transfer"
    }
  ]
}' localhost:3000 torii.Torii/SubscribeToTopicsStream

# Subscribe with filters
grpcurl -plaintext -d '{
  "clientId": "my-client",
  "topics": [
    {
      "topic": "erc20.transfer",
      "filters": {
        "wallet": "0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7"
      }
    }
  ]
}' localhost:3000 torii.Torii/SubscribeToTopicsStream
```

---

## Filter Reference

### ERC20 TransferFilter

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` (wallet-centric) |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `tokens` | bytes[] | Token contract whitelist |
| `direction` | enum | `DIRECTION_ALL`, `DIRECTION_SENT`, `DIRECTION_RECEIVED` |
| `blockFrom` | uint64 | Minimum block number (inclusive) |
| `blockTo` | uint64 | Maximum block number (inclusive) |

### ERC20 ApprovalFilter

| Field | Type | Description |
|-------|------|-------------|
| `account` | bytes | Matches `owner` OR `spender` |
| `owner` | bytes | Exact owner address |
| `spender` | bytes | Exact spender address |
| `tokens` | bytes[] | Token contract whitelist |
| `blockFrom` | uint64 | Minimum block number (inclusive) |
| `blockTo` | uint64 | Maximum block number (inclusive) |

### ERC721 TransferFilter

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `tokens` | bytes[] | NFT contract whitelist |
| `tokenIds` | bytes[] | Specific NFT token IDs |
| `blockFrom` | uint64 | Minimum block number (inclusive) |
| `blockTo` | uint64 | Maximum block number (inclusive) |

### ERC721 OwnershipFilter

| Field | Type | Description |
|-------|------|-------------|
| `owner` | bytes | Owner address |
| `tokens` | bytes[] | NFT contract whitelist |
| `tokenIds` | bytes[] | Specific NFT token IDs |

### ERC1155 TransferFilter

| Field | Type | Description |
|-------|------|-------------|
| `wallet` | bytes | Matches `from` OR `to` |
| `from` | bytes | Exact sender address |
| `to` | bytes | Exact receiver address |
| `operator` | bytes | Exact operator address |
| `tokens` | bytes[] | Token contract whitelist |
| `tokenIds` | bytes[] | Specific token IDs |
| `blockFrom` | uint64 | Minimum block number (inclusive) |
| `blockTo` | uint64 | Maximum block number (inclusive) |

---

## Well-Known Addresses (Base64)

| Token | Address (Hex) | Address (Base64) |
|-------|---------------|------------------|
| ETH | `0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7` | `BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=` |
| STRK | `0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D` | `BHGPWgPDTMGvFqHN7pj/sgwx9c1h1mqwcgGFj0KHyY0=` |

### Converting Addresses

```bash
# Hex to Base64
echo -n "049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7" | xxd -r -p | base64

# Base64 to Hex
echo "BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=" | base64 -d | xxd -p
```

### Converting U256 Values

```bash
# Integer to Base64 (example: 1000000000000000000 = 1 ETH in wei)
printf '%016x' 1000000000000000000 | xxd -r -p | base64
# Output: DeC2s6dkAAA=

# Base64 to Integer
echo "DeC2s6dkAAA=" | base64 -d | xxd -p
# Output: 0de0b6b3a7640000
# Then: python3 -c "print(int('0de0b6b3a7640000', 16))"
# Output: 1000000000000000000
```
