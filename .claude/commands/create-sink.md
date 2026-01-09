# Create Custom Sink

Guide the user through creating a custom Torii sink. A Sink processes decoded blockchain events and exposes functionality through EventBus (real-time subscriptions), HTTP (REST endpoints), and/or gRPC (RPC services).

## Instructions

When the user invokes this skill, help them create a custom sink by:

1. **Ask what the sink should do** - storage type, event types, output channels (EventBus/HTTP/gRPC)
2. **Generate the implementation** following the patterns below
3. **Create the decoder** if needed
4. **Show registration** with ToriiConfig

## Sink Trait Reference

Location: `src/etl/sink/mod.rs:37-149`

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    /// Unique sink identifier
    fn name(&self) -> &str;

    /// TypeIds this sink processes (from decoder)
    fn interested_types(&self) -> Vec<TypeId>;

    /// EventBus topics with available filters
    fn topics(&self) -> Vec<TopicInfo>;

    /// Called once at startup with EventBus access
    async fn initialize(&mut self, event_bus: Arc<EventBus>) -> Result<()>;

    /// Process decoded envelopes each ETL cycle
    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()>;

    /// Return Axum Router for HTTP endpoints
    fn build_routes(&self) -> Router;
}
```

## Minimal Sink Template

```rust
use async_trait::async_trait;
use axum::Router;
use std::sync::Arc;
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, TopicInfo},
};

pub struct MySink {
    event_bus: Option<Arc<EventBus>>,
}

impl MySink {
    pub fn new() -> Self {
        Self { event_bus: None }
    }
}

#[async_trait]
impl Sink for MySink {
    fn name(&self) -> &str {
        "my-sink"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("my.event")]
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "my-topic",
            vec!["filter1".to_string()],
            "Description of this topic",
        )]
    }

    async fn initialize(&mut self, event_bus: Arc<EventBus>) -> anyhow::Result<()> {
        self.event_bus = Some(event_bus);
        Ok(())
    }

    async fn process(
        &self,
        envelopes: &[Envelope],
        _batch: &ExtractionBatch,
    ) -> anyhow::Result<()> {
        for envelope in envelopes {
            if envelope.type_id == TypeId::new("my.event") {
                if let Some(data) = envelope.downcast_ref::<MyEvent>() {
                    // 1. Store data (database, memory, etc.)
                    // 2. Publish to EventBus if needed
                    // 3. Broadcast to gRPC subscribers if needed
                }
            }
        }
        Ok(())
    }

    fn build_routes(&self) -> Router {
        Router::new()
        // Add routes like:
        // .route("/my-sink/data", get(handler))
        // .with_state(state)
    }
}
```

## Decoder Template

Decoders transform raw Starknet events into typed Envelopes:

```rust
use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use starknet::core::utils::starknet_keccak;
use torii::etl::{
    decoder::Decoder,
    envelope::{Envelope, EnvelopeMetadata, TypeId, TypedBody},
};

#[derive(Debug, Clone)]
pub struct MyEvent {
    pub field1: String,
    pub field2: u64,
}

impl TypedBody for MyEvent {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("my.event")
    }
}

pub struct MyDecoder;

#[async_trait]
impl Decoder for MyDecoder {
    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let my_selector = starknet_keccak("MyEvent".as_bytes());

        let envelopes: Vec<Envelope> = events
            .iter()
            .enumerate()
            .filter_map(|(idx, event)| {
                let selector = event.keys.first()?;
                if *selector != my_selector {
                    return None;
                }

                // Parse event data
                let field1 = parse_field(&event.keys, 1)?;
                let field2 = parse_u64(&event.data, 0)?;

                let metadata = EnvelopeMetadata {
                    block_number: event.block_number,
                    from_address: event.from_address,
                    // ... other fields
                };

                Some(Envelope::new(
                    format!("my_event_{}", idx),
                    Box::new(MyEvent { field1, field2 }),
                    metadata,
                ))
            })
            .collect();

        Ok(envelopes)
    }
}
```

## Registration with ToriiConfig

```rust
use torii::{run, ToriiConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sink = MySink::new();
    let decoder = Arc::new(MyDecoder);

    let config = ToriiConfig::builder()
        .port(8080)
        .add_sink_boxed(Box::new(sink))
        .add_decoder(decoder)
        .build();

    run(config).await
}
```

## With gRPC Service

If sink provides a gRPC service:

```rust
use tonic::transport::Server;

let sink = MySink::new();
let grpc_service = sink.get_grpc_service_impl();

let grpc_router = Server::builder()
    .accept_http1(true)
    .add_service(tonic_web::enable(MySinkServer::new(grpc_service)));

let config = ToriiConfig::builder()
    .port(8080)
    .add_sink_boxed(Box::new(sink))
    .add_decoder(Arc::new(MyDecoder))
    .with_grpc_router(grpc_router)
    .build();
```

## EventBus Publishing

Publish to EventBus for real-time subscriptions:

```rust
if let Some(event_bus) = &self.event_bus {
    let proto_msg = MyProtoMessage { ... };
    let mut buf = Vec::new();
    proto_msg.encode(&mut buf)?;

    let any = prost_types::Any {
        type_url: "type.googleapis.com/my.package.MyMessage".to_string(),
        value: buf,
    };

    event_bus.publish_protobuf(
        "my-topic",
        &envelope.id,
        &any,
        &proto_msg,
        UpdateType::Created,
        |msg: &MyProtoMessage, filters| {
            // Return true if msg matches client's subscription filters
            true
        },
    );
}
```

## Reference Implementations

- **SQL Sink**: `crates/torii-sql-sink/src/lib.rs` - Full-featured with SQLite, gRPC, HTTP, EventBus
- **Log Sink**: `crates/torii-log-sink/src/lib.rs` - In-memory storage example
- **Examples**: `examples/simple_sql_sink/`, `examples/eventbus_only_sink/`, `examples/http_only_sink/`

## Sink Types

| Type | EventBus | HTTP | gRPC | Storage |
|------|----------|------|------|---------|
| Full | Yes | Yes | Yes | Database |
| EventBus-only | Yes | No | No | None |
| HTTP-only | No | Yes | No | Memory |
| Storage-only | No | No | No | Database |
