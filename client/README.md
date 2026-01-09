# Torii Client

TypeScript/Svelte demo client with gRPC-web support for real-time blockchain event streaming with Torii.

## Features

- ✅ Real-time gRPC-web streaming (server-side streaming)
- ✅ Multi-topic subscriptions with custom filters
- ✅ Protobuf message decoding (SQL operations, logs)
- ✅ Direct sink gRPC services (SQL queries, log queries)
- ✅ HTTP REST API integration
- ✅ Clean, functional UI

## Quick Start

```bash
pnpm install
pnpm run proto:generate

# Start torii
cargo run --example multi_sink_example

# Start the client
pnpm run dev
```

## Development

### Proto Generation

The `proto:generate` script:
1. Copies proto files from `../proto` to `./proto`
2. Generates TypeScript code using `@protobuf-ts`
3. Creates type-safe clients and message definitions

```bash
pnpm run proto:generate
```

Generated files are in `src/generated/`
