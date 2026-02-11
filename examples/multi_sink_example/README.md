# Multi-Sink Example

This example demonstrates using **multiple custom sinks** (SqlSink + LogSink) in a single Torii instance.

## What This Shows

1. **Multiple Sinks** - SqlSink and LogSink running together
2. **Multiple gRPC Services** - All on the same port (8080):
   - `torii.Torii` (core service)
   - `torii.sinks.sql.SqlSink`
   - `torii.sinks.log.LogSink`
3. **Multiple HTTP Routes** - Automatically merged:
   - SqlSink: `/sql/query`, `/sql/events`
   - LogSink: `/logs`, `/logs/count`
4. **Multiple Decoders** - SqlDecoder and LogDecoder processing the same events
5. **EventBus Topics** - Both sinks publishing to their topics (`sql` and `logs`)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Torii Multi-Sink                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Events → ┌──────────────┐ → SqlSink  → SQLite            │
│           │  SqlDecoder  │                                  │
│           └──────────────┘                                  │
│                                                             │
│  Events → ┌──────────────┐ → LogSink  → Memory             │
│           │  LogDecoder  │                                  │
│           └──────────────┘                                  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            All Services on Port 8080                 │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ gRPC:  torii.Torii                                   │  │
│  │        torii.sinks.sql.SqlSink                       │  │
│  │        torii.sinks.log.LogSink                       │  │
│  │                                                      │  │
│  │ HTTP:  /sql/query, /sql/events                      │  │
│  │        /logs, /logs/count                           │  │
│  │                                                      │  │
│  │ EventBus: "sql", "logs"                             │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Running

```bash
cargo run -p torii --example multi_sink_example
```

The server will start on `http://0.0.0.0:8080` and display a comprehensive testing guide.

## Testing gRPC (direct sink services)

### Verify services are running

```bash
grpcurl -plaintext localhost:8080 list
```

Expected output:
```
grpc.reflection.v1.ServerReflection
torii.Torii
torii.sinks.log.LogSink
torii.sinks.sql.SqlSink
```

### Call Sink Services Directly

#### SQL Sink - Query Operations

```bash
grpcurl -plaintext -d '{"limit":5}' localhost:8080 torii.sinks.sql.SqlSink/Query
```

#### SQL Sink - Subscribe to Operations

```bash
grpcurl -plaintext localhost:8080 torii.sinks.sql.SqlSink/Subscribe
```

#### Log Sink - Query Logs

```bash
grpcurl -plaintext -d '{"limit":5}' localhost:8080 torii.sinks.log.LogSink/QueryLogs
```

#### Log Sink - Subscribe to Logs (Real-time Stream)

```bash
grpcurl -plaintext localhost:8080 torii.sinks.log.LogSink/SubscribeLogs
```

## Testing HTTP

### Log Sink Endpoints

```bash
# Get recent logs (default: 5, max: 100)
curl "http://localhost:8080/logs?limit=10"

# Get total log count
curl "http://localhost:8080/logs/count"
```

### SQL Sink Endpoints

```bash
# List all SQL operations
curl "http://localhost:8080/sql/events"

# Execute SQL query
curl -X POST "http://localhost:8080/sql/query" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM sql_operation ORDER BY created_at DESC LIMIT 5"}'
```

## Testing EventBus (Central Subscriptions)

The EventBus allows clients to subscribe to multiple topics in a single connection:

```bash
# Subscribe to both SQL and logs topics
grpcurl -plaintext -d '{
  "client_id": "test",
  "topics": [
      {
      "topic": "logs",
      "filters": {},
      "filterData": {
        "@type": "type.googleapis.com/google.protobuf.StringValue",
        "value": "test"}
    },
    {
      "topic": "sql",
      "filters": {},
      "filterData": {
        "@type": "type.googleapis.com/google.protobuf.StringValue",
        "value": "test"}
    }
  ],
  "unsubscribe_topics": []
}' localhost:8080 torii.Torii/SubscribeToTopicsStream
```

With filters:

```bash
# Only SQL operations with value > 100
grpcurl -plaintext -d '{
  "client_id": "test",
  "topics": [
    {
      "topic": "sql",
      "filters": {
        "value_gt": "100"
      },
      "filterData": {
        "@type": "type.googleapis.com/google.protobuf.StringValue",
        "value": "test"}
    }
  ],
  "unsubscribe_topics": []
}' localhost:8080 torii.Torii/SubscribeToTopicsStream
```

## Key Learnings

### 1. gRPC Service Registration

Both sinks register their gRPC services **before** passing the router to Torii:

```rust
let grpc_router = Server::builder()
    .accept_http1(true)
    .add_service(tonic_web::enable(SqlSinkServer::new(sql_service)))
    .add_service(tonic_web::enable(LogSinkServer::new(log_service)));
    // Torii adds core service to this router
```

### 2. HTTP Route Merging

HTTP routes from both sinks are automatically merged by Torii:

```rust
// SqlSink provides: /sql/query, /sql/events
// LogSink provides: /logs, /logs/count
// All accessible on the same port
```

### 3. Multiple Decoders

Both decoders process **the same events**, each extracting different information:

```rust
// SqlDecoder: Events → SQL operations
// LogDecoder: Events → Log entries
```

### 4. Two Publishing Mechanisms

Each sink uses **two** ways to publish data:

1. **EventBus** (central) - Via `torii.Torii/Subscribe`:
   - Single connection for multiple topics
   - Sink-defined filtering
   - Topic: "sql", "logs"

2. **Sink-specific gRPC** - Via `torii.sinks.{sink}.{Service}/Subscribe`:
   - Dedicated connection per sink
   - Sink-specific API
   - No filtering (gets all updates)
