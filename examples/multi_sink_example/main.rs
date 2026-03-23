//! Example: Torii server with multiple sinks (SqlSink + LogSink).
//!
//! This demonstrates:
//! 1. Registering multiple custom sinks (SqlSink + LogSink)
//! 2. Multiple gRPC services on the same port
//! 3. Multiple HTTP routers automatically merged
//! 4. Complete multi-sink integration
//!
//! Run: `cargo run --example multi_sink_example`
//!
//! With this example, you can use the README to test in the terminal,
//! or also spin up the client and test in the browser (inside `client` directory).

use std::sync::Arc;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use torii::{run, ToriiConfig};
use torii_log_sink::{LogDecoder, LogSink};
use torii_sql_sink::{SqlDecoder, SqlSink};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Torii Multi-Sink Example - SqlSink + LogSink\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. CREATE SINKS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    println!("📦 Creating sinks...");

    // Create SQL sink (DATABASE_URL or in-memory SQLite fallback)
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    let sql_sink = SqlSink::new(&database_url).await?;
    let sql_grpc_service = sql_sink.get_grpc_service_impl();
    println!("   ✅ SqlSink created ({database_url})");

    // Create Log sink (in-memory, max 100 logs)
    let log_sink = LogSink::new(100);
    let log_grpc_service = log_sink.get_grpc_service_impl();
    println!("   ✅ LogSink created (max 100 logs in memory)\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. BUILD gRPC ROUTER WITH MULTIPLE SERVICES + REFLECTION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    println!("🔧 Building gRPC router with multiple sink services + reflection...");

    let grpc_router = {
        use torii_log_sink::proto::log_sink_server::LogSinkServer;
        use torii_sql_sink::proto::sql_sink_server::SqlSinkServer;

        // Build reflection service with ALL descriptor sets
        // This makes ALL services discoverable via grpcurl list!
        // Use torii::TORII_DESCRIPTOR_SET (exported from torii crate)
        let reflection_v1 = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET) // Core Torii
            .register_encoded_file_descriptor_set(torii_sql_sink::FILE_DESCRIPTOR_SET) // SQL Sink
            .register_encoded_file_descriptor_set(torii_log_sink::FILE_DESCRIPTOR_SET) // Log Sink
            .build_v1()
            .expect("Failed to build reflection v1")
            .accept_compressed(CompressionEncoding::Gzip);

        let reflection_v1alpha = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET) // Core Torii
            .register_encoded_file_descriptor_set(torii_sql_sink::FILE_DESCRIPTOR_SET) // SQL Sink
            .register_encoded_file_descriptor_set(torii_log_sink::FILE_DESCRIPTOR_SET) // Log Sink
            .build_v1alpha()
            .expect("Failed to build reflection v1alpha")
            .accept_compressed(CompressionEncoding::Gzip);

        let sql_service = SqlSinkServer::new((*sql_grpc_service).clone())
            .accept_compressed(CompressionEncoding::Gzip);
        let log_service = LogSinkServer::new((*log_grpc_service).clone())
            .accept_compressed(CompressionEncoding::Gzip);

        Server::builder()
            // Accept HTTP/1.1 requests required for gRPC-Web to work.
            .accept_http1(true)
            // Add sink services.
            .add_service(tonic_web::enable(sql_service))
            .add_service(tonic_web::enable(log_service))
            // Add reflection services with ALL descriptors.
            .add_service(tonic_web::enable(reflection_v1))
            .add_service(tonic_web::enable(reflection_v1alpha))
        // Torii will add the core service to this router (but NOT reflection, we already have it).
    };

    println!("   ✅ gRPC router built with:");
    println!("      • torii.sinks.sql.SqlSink");
    println!("      • torii.sinks.log.LogSink");
    println!("      • Reflection (with ALL service descriptors)\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3. CREATE DECODERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    println!("🔍 Creating decoders...");

    // No filter -> all events for both.
    let sql_decoder = Arc::new(SqlDecoder::new(Vec::new()));
    let log_decoder = Arc::new(LogDecoder::new(None));

    println!("   ✅ SqlDecoder created (no filters)");
    println!("   ✅ LogDecoder created (no filters)\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 4. GET SAMPLE EVENTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    // SQL sink provides sample events which will allow some automatic event
    // generation and testing.
    let sample_events = SqlSink::generate_sample_events();
    println!("📋 Loaded {} sample event types\n", sample_events.len());

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 5. CONFIGURE TORII
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    println!("⚙️  Configuring Torii...");

    let config = ToriiConfig::builder()
        .port(8080)
        .host("0.0.0.0".to_string())
        // Add sinks
        .add_sink_boxed(Box::new(sql_sink))
        .add_sink_boxed(Box::new(log_sink))
        // Add decoders
        .add_decoder(sql_decoder)
        .add_decoder(log_decoder)
        // Add gRPC router with sink services + reflection
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true) // We already added reflection with all descriptors
        // Add sample events for testing
        .with_sample_events(sample_events)
        // ETL configuration
        .cycle_interval(3) // Generate events every 3 seconds
        .events_per_cycle(2) // 2 events per cycle
        .build();

    println!("   ✅ Configuration complete\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 6. PRINT SUMMARY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 TORII MULTI-SINK CONFIGURATION");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🌐 Server: http://0.0.0.0:8080");
    println!("🔄 ETL Cycle: {} seconds", config.cycle_interval);
    println!("📊 Events/Cycle: {}", config.events_per_cycle);
    println!();
    println!("📍 Sinks: {}", config.sinks.len());
    println!("   1. SqlSink - SQL operations storage");
    println!("   2. LogSink - Event log collection");
    println!();
    println!("🔍 Decoders: {}", config.decoders.len());
    println!("   1. SqlDecoder - Events → SQL operations");
    println!("   2. LogDecoder - Events → Log entries");
    println!();
    println!("📡 gRPC Services:");
    println!("   • torii.Torii (core)");
    println!("   • torii.sinks.sql.SqlSink");
    println!("   • torii.sinks.log.LogSink");
    println!();
    println!("🌐 HTTP Endpoints:");
    println!("   SqlSink:");
    println!("   • POST /sql/query     - Execute SQL queries");
    println!("   • GET  /sql/events    - List SQL operations");
    println!("   LogSink:");
    println!("   • GET  /logs          - Get recent logs");
    println!("   • GET  /logs/count    - Get total log count");
    println!();
    println!("🔔 EventBus Topics:");
    println!("   • sql  - SQL operations (with filters)");
    println!("   • logs - Log entries");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🧪 TESTING GUIDE");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("📡 gRPC Testing:");
    println!("   # List all services (ALL services will appear!)");
    println!("   grpcurl -plaintext localhost:8080 list");
    println!("   # Expected output:");
    println!("   #   grpc.reflection.v1.ServerReflection");
    println!("   #   grpc.reflection.v1alpha.ServerReflection");
    println!("   #   torii.Torii                              ← Core");
    println!("   #   torii.sinks.sql.SqlSink                  ← SQL Sink ✨");
    println!("   #   torii.sinks.log.LogSink                  ← Log Sink ✨");
    println!();
    println!("   # Query SQL operations");
    println!(
        "   grpcurl -plaintext -d '{{\"limit\":5}}' localhost:8080 torii.sinks.sql.SqlSink/Query"
    );
    println!();
    println!("   # Query logs");
    println!(
        "   grpcurl -plaintext -d '{{\"limit\":5}}' localhost:8080 torii.sinks.log.LogSink/QueryLogs"
    );
    println!();
    println!("   # Subscribe to SQL operations");
    println!("   grpcurl -plaintext localhost:8080 torii.sinks.sql.SqlSink/Subscribe");
    println!();
    println!("   # Subscribe to logs");
    println!("   grpcurl -plaintext localhost:8080 torii.sinks.log.LogSink/SubscribeLogs");
    println!();
    println!("🌐 HTTP Testing:");
    println!("   # Get recent logs");
    println!("   curl http://localhost:8080/logs?limit=5");
    println!();
    println!("   # Get log count");
    println!("   curl http://localhost:8080/logs/count");
    println!();
    println!("   # Query SQL operations");
    println!("   curl http://localhost:8080/sql/events");
    println!();
    println!("   # Execute SQL query");
    println!(
        r#"   curl -X POST http://localhost:8080/sql/query -d '{{"query":"SELECT * FROM sql_operation LIMIT 5"}}'"#
    );
    println!();
    println!("🎨 Frontend (Optional):");
    println!("   cd client");
    println!("   npm run dev");
    println!("   Open http://localhost:5173");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🚀 Starting server...\n");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 7. RUN SERVER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    run(config).await
}
