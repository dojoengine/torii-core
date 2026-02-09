# Torii Tokens Frontend Clients

Multi-framework showcase for the Torii Tokens indexer (ERC20/ERC721/ERC1155).

## Project Structure

```
client/
├── package.json              # Root workspace (bun workspaces)
├── shared/                   # Shared package
│   ├── src/
│   │   ├── client.ts        # Client factory
│   │   ├── utils.ts         # Address/format utilities
│   │   └── index.ts
│   └── styles/
│       └── shared.css       # CSS variables, base styles
├── apps/
│   ├── vanilla/             # Pure TypeScript + DOM
│   ├── svelte/              # Svelte 5 + Vite
│   └── react/               # React 18 + Vite
└── README.md
```

## Quick Start

### 1. Install Dependencies

```bash
cd bins/torii-tokens/client
bun install
```

### 2. Generate Clients

Make sure the torii-tokens server is running:

```bash
# From repository root
cargo run --bin torii-tokens -- --from-block 0
```

Generate TypeScript clients from the gRPC schema:

```bash
bun run generate
```

### 3. Run Applications

Each app runs on a different port:

```bash
# Vanilla TypeScript (http://localhost:5173)
bun run dev:vanilla

# Svelte 5 (http://localhost:5174)
bun run dev:svelte

# React 18 (http://localhost:5175)
bun run dev:react
```

## Features

All three applications showcase:

- **Status Panel**: Connection status, server info, client ID
- **ERC20 Panel**: Transfer stats, transfers table, approvals
- **ERC721 Panel**: NFT stats, transfers, ownership info
- **ERC1155 Panel**: Multi-token stats, transfers
- **Updates Feed**: Real-time subscription updates

## Shared Package

The `@torii-tokens/shared` package provides:

### Client Factory

```typescript
import { createTokensClient, SERVER_URL } from "@torii-tokens/shared";

const client = createTokensClient(SERVER_URL);

// Get server version
const version = await client.getVersion();

// Subscribe to topics
const unsubscribe = await client.subscribeTopics(
  "my-client-id",
  [{ topic: "erc20.transfer" }],
  (update) => console.log("Update:", update),
  (error) => console.error("Error:", error),
  () => console.log("Connected!")
);
```

### Utilities

```typescript
import {
  hexToBase64,      // Convert hex address to base64 for API
  base64ToHex,      // Convert base64 to hex for display
  formatU256,       // Format token amounts
  formatTimestamp,  // Human-readable timestamps
  truncateAddress,  // 0x123...abc
  getUpdateTypeName,// CREATED/UPDATED/DELETED
  generateClientId, // Random client ID
} from "@torii-tokens/shared";
```

### Shared Styles

Import the shared CSS for consistent styling:

```html
<link rel="stylesheet" href="../../shared/styles/shared.css" />
```

CSS variables available:
- `--color-bg`, `--color-surface`, `--color-text-*`
- `--color-primary`, `--color-success`, `--color-danger`
- `--gradient-header`, `--gradient-erc20/721/1155`
- `--shadow-sm`, `--shadow-md`
- `--radius-sm`, `--radius-md`, `--radius-lg`

## Development

### Build All Apps

```bash
bun run build
```

### Regenerate Clients

When the server schema changes:

```bash
bun run generate
```

## API Reference

See [bins/torii-tokens/README.md](../README.md) for the full gRPC API reference.

### Address Encoding

All addresses in the API are base64-encoded big-endian bytes:

```typescript
// Convert hex to base64 for API requests
const address = hexToBase64("0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7");

// Convert base64 back to hex for display
const hex = base64ToHex("BJ02Vw1ORvSOmWdL0/zIRkTd1rlvfHQbFWK4L54ATcc=");
```
