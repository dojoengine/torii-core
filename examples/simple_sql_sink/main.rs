// Example: Complete Torii server with SqlSink
//
// This demonstrates the new modular architecture:
// 1. Create sink + decoder
// 2. Configure Torii
// 3. Run server (auto-discovers gRPC services, HTTP routes, EventBus topics)
//
// Run: cargo run --example simple_sql_sink
// Then: Open http://localhost:5173 in browser (after starting client)

use std::sync::Arc;
use torii::{ToriiConfig, run};
use torii_sql_sink::{SqlDecoder, SqlSink};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Torii SQL Sink Example - Starting Server...\n");

    // 1. Create sink
    let sql_sink = SqlSink::new("sqlite::memory:").await?;

    // 2. Get the gRPC service implementation
    let sql_grpc_service = sql_sink.get_grpc_service_impl();

    // 3. Build gRPC router with SQL sink service
    // This happens in user code where Rust's type inference can see all types
    let grpc_router = {
        use torii_sql_sink::proto::sql_sink_server::SqlSinkServer;

        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(SqlSinkServer::new((*sql_grpc_service).clone())))
        // Torii will add the core service to this router
    };

    // 4. Create decoder
    let sql_decoder = Arc::new(SqlDecoder::new(Vec::new())); // No filters = all events

    // 5. Get sample events
    let sample_events = SqlSink::generate_sample_events();

    // 6. Configure Torii with builder pattern
    let config = ToriiConfig::builder()
        .port(8080)
        .host("0.0.0.0".to_string())
        .add_sink_boxed(Box::new(sql_sink))
        .add_decoder(sql_decoder)
        .with_grpc_router(grpc_router)  // Pass router with sink services
        .with_sample_events(sample_events)
        .cycle_interval(3)  // Generate events every 3 seconds
        .events_per_cycle(2)  // 2 events per cycle
        .build();

    println!("Server Configuration:");
    println!("   • Port: {}", config.port);
    println!("   • Sinks: {}", config.sinks.len());
    println!("   • Decoders: {}", config.decoders.len());
    println!("   • ETL Cycle: {} seconds", config.cycle_interval);
    println!("   • Events/Cycle: {}\n", config.events_per_cycle);

    println!("What this sink provides:");
    println!("   1. EventBus → Central topic subscriptions (torii.Torii/Subscribe)");
    println!("   2. gRPC Service → torii.sinks.sql.SqlSink");
    println!("      - Query: Execute SQL queries");
    println!("      - StreamQuery: Stream large result sets");
    println!("      - GetSchema: Get database schema");
    println!("      - Subscribe: Real-time operation updates");
    println!("   3. REST HTTP → /sql/query, /sql/events\n");

    println!("Frontend:");
    println!("   1. cd client");
    println!("   2. npm run dev");
    println!("   3. Open http://localhost:5173\n");

    println!("Starting server...\n");

    // 5. Run! (auto-discovers and registers everything)
    run(config).await
}
