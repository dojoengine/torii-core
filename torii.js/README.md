# torii.js

TypeScript/JavaScript client library and CLI for Torii gRPC services.

## Installation

```bash
bun add torii.js
# or
npm install torii.js
```

## CLI Usage

### Generate from gRPC Reflection

Connect to a running Torii server and generate clients from reflection:

```bash
bunx torii.js --url http://localhost:8080
```

### Generate from Proto Files

Generate clients from local .proto files:

```bash
bunx torii.js ./path/to/protos
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `[path]` | Path to search for .proto files | - |
| `--url` | Server URL for reflection | `http://localhost:8080` |
| `--output`, `-o` | Output directory | `./generated` |

## Client Usage

```typescript
import { ToriiClient } from './generated';

const client = new ToriiClient('http://localhost:8080');

// Get server version
const version = await client.getVersion();
console.log(version);

// List available topics
const topics = await client.listTopics();

// Subscribe to updates
await client.subscribe(
  [{ topic: 'sql', filters: { table: 'users' } }],
  (update) => console.log('Update:', update),
  (error) => console.error('Error:', error)
);

// SQL queries (if SqlSink available)
const result = await client.sql?.query('SELECT * FROM users');

// Stream large result sets
await client.sql?.streamQuery(
  'SELECT * FROM events',
  (row) => console.log(row),
  () => console.log('Done')
);
```

## Architecture

torii.js generates TypeScript clients from either:
1. **gRPC Reflection** - Discovers services at runtime from a running server
2. **Proto Files** - Compiles .proto files directly

The generated `ToriiClient` aggregates all discovered services into a single entrypoint.
