use crate::etl::engine_db::EngineDbConfig;
use crate::etl::extractor::Extractor;
use crate::etl::sink::{EventBus, Sink};
use crate::etl::{DecoderContext, EngineDb, MultiSink, SampleExtractor};
use crate::{create_grpc_service, GrpcState, SubscriptionManager};
use crate::{create_http_router, ToriiConfig};
use crate::{metrics, FILE_DESCRIPTOR_SET};
use axum::Router as AxumRouter;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tower_http::cors::{Any as CorsAny, CorsLayer};

/// Starts the Torii server with custom configuration.
///
/// NOTE: The caller is responsible for initializing the tracing subscriber before calling this function.
///
/// TODO: this function is just too big. But it has the whole workflow.
/// This will be split into smaller functions in the future with associated configuration for each step.
pub async fn run(config: ToriiConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(target: "torii::main", "Starting Torii with {} sink(s) and {} decoder(s)",
        config.sinks.len(), config.decoders.len());

    match metrics::init_from_env() {
        Ok(true) => {
            tracing::info!(target: "torii::main", "Prometheus metrics enabled at /metrics");
            metrics::set_build_info(env!("CARGO_PKG_VERSION"));
        }
        Ok(false) => {
            tracing::info!(target: "torii::main", "Prometheus metrics disabled by TORII_METRICS_ENABLED");
        }
        Err(e) => {
            tracing::warn!(target: "torii::main", error = %e, "Failed to initialize metrics recorder");
        }
    }

    let subscription_manager = Arc::new(SubscriptionManager::new());
    let event_bus = Arc::new(EventBus::new(subscription_manager.clone()));

    // Create SinkContext for initialization
    let sink_context = etl::sink::SinkContext {
        database_root: config.database_root.clone(),
    };

    let mut initialized_sinks: Vec<Arc<dyn Sink>> = Vec::new();

    for mut sink in config.sinks {
        // Box is used for sinks since we need to call initialize (mutable reference).
        sink.initialize(event_bus.clone(), &sink_context).await?;
        // Convert Box<dyn Sink> to Arc<dyn Sink> since now we can use it immutably.
        initialized_sinks.push(Arc::from(sink));
    }

    let multi_sink = Arc::new(MultiSink::new(initialized_sinks));

    // Create EngineDb (needed by DecoderContext)
    let engine_db_path = config.engine_database_url.clone().unwrap_or_else(|| {
        config
            .database_root
            .join("engine.db")
            .to_string_lossy()
            .to_string()
    });
    let engine_db_config = EngineDbConfig {
        path: engine_db_path,
    };
    let engine_db = EngineDb::new(engine_db_config).await?;
    let engine_db = Arc::new(engine_db);

    // Create extractor early so we can get the provider for contract identification
    let extractor: Box<dyn Extractor> = if let Some(extractor) = config.extractor {
        tracing::info!(target: "torii::etl", "Using configured extractor");
        extractor
    } else {
        tracing::info!(target: "torii::etl", "No extractor configured, using SampleExtractor for testing");
        if config.sample_events.is_empty() {
            tracing::warn!(target: "torii::etl", "No sample events provided, ETL loop will idle");
        } else {
            tracing::info!(
                target: "torii::etl",
                "Loaded {} sample event types (will cycle through them)",
                config.sample_events.len()
            );
        }
        Box::new(SampleExtractor::new(
            config.sample_events,
            config.events_per_cycle,
        ))
    };

    // Create DecoderContext with contract filtering and optional registry
    let decoder_context = if let Some(registry_cache) = config.registry_cache {
        tracing::info!(
            target: "torii::etl",
            "Creating DecoderContext with registry cache (auto-identification enabled)"
        );
        DecoderContext::with_registry(
            config.decoders,
            engine_db.clone(),
            config.contract_filter,
            registry_cache,
        )
    } else {
        tracing::info!(
            target: "torii::etl",
            "Creating DecoderContext without registry (all decoders for unmapped contracts)"
        );
        DecoderContext::new(config.decoders, engine_db.clone(), config.contract_filter)
    };

    let topics = multi_sink.topics();

    let grpc_state = GrpcState::new(subscription_manager.clone(), topics);
    let grpc_service = create_grpc_service(grpc_state);

    let has_user_grpc_services = config.partial_grpc_router.is_some();
    let mut grpc_router = if let Some(partial_router) = config.partial_grpc_router {
        tracing::info!(target: "torii::main", "Using user-provided gRPC router with sink services");
        partial_router.add_service(tonic_web::enable(grpc_service))
    } else {
        Server::builder()
            // Accept HTTP/1.1 requests required for gRPC-Web to work.
            .accept_http1(true)
            .add_service(tonic_web::enable(grpc_service))
    };

    if config.custom_reflection {
        tracing::info!(target: "torii::main", "Using custom reflection services (user-provided)");
    } else {
        let reflection_v1 = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        let reflection_v1alpha = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1alpha()?;

        grpc_router = grpc_router
            .add_service(tonic_web::enable(reflection_v1))
            .add_service(tonic_web::enable(reflection_v1alpha));

        tracing::info!(target: "torii::main", "Added reflection services (core descriptors only)");
    }

    let sinks_routes = multi_sink.build_routes();
    let http_router = create_http_router().merge(sinks_routes);

    let cors = CorsLayer::new()
        .allow_origin(CorsAny)
        .allow_methods(CorsAny)
        .allow_headers(CorsAny)
        .expose_headers(vec![
            axum::http::HeaderName::from_static("grpc-status"),
            axum::http::HeaderName::from_static("grpc-message"),
            axum::http::HeaderName::from_static("grpc-status-details-bin"),
            axum::http::HeaderName::from_static("x-grpc-web"),
            axum::http::HeaderName::from_static("content-type"),
        ]);

    // Until some compatibility issues are resolved with axum, we need to allow this deprecated code.
    // See: https://github.com/hyperium/tonic/issues/1964.
    #[allow(warnings, deprecated)]
    let app = AxumRouter::new()
        .merge(grpc_router.into_router())
        .merge(http_router)
        .layer(cors);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    tracing::info!(target: "torii::main", "Server listening on {}", addr);

    tracing::info!(target: "torii::main", "gRPC Services:");
    tracing::info!(target: "torii::main", "   torii.Torii - Core service");
    if has_user_grpc_services {
        tracing::info!(target: "torii::main", "   + User-provided sink gRPC services");
    }

    // Create cancellation token for graceful shutdown coordination.
    let shutdown_token = CancellationToken::new();

    // Setup and start the ETL pipeline.
    let etl_multi_sink = multi_sink.clone();
    let etl_engine_db = engine_db.clone();
    let cycle_interval = config.cycle_interval;
    let etl_shutdown_token = shutdown_token.clone();
    let etl_concurrency = config.etl_concurrency.clone();

    // Move multi_decoder into the task (can't clone since it owns the registry)
    let etl_decoder_context = decoder_context;

    // Optional contract identifier for runtime identification
    let contract_identifier = config.contract_identifier;

    // Extractor was already created earlier (to get provider), make it mutable for the ETL loop
    let extractor = Arc::new(tokio::sync::Mutex::new(extractor));

    let etl_handle = tokio::spawn(async move {
        tracing::info!(target: "torii::etl", "Starting ETL pipeline...");

        // Wait a bit for the server to be ready.
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        #[derive(Debug)]
        struct PrefetchedBatch {
            batch: etl::extractor::ExtractionBatch,
            cursor: Option<String>,
            extractor_finished: bool,
        }

        let prefetch_capacity = etl_concurrency.resolved_prefetch_batches();
        let (prefetch_tx, mut prefetch_rx) =
            tokio::sync::mpsc::channel::<PrefetchedBatch>(prefetch_capacity);
        let queue_depth = Arc::new(AtomicUsize::new(0));

        let (identify_tx, identify_handle) = if let Some(identifier) = contract_identifier.clone() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<starknet::core::types::Felt>>(
                prefetch_capacity.saturating_mul(2).max(8),
            );
            let handle = tokio::spawn(async move {
                while let Some(contract_addresses) = rx.recv().await {
                    if contract_addresses.is_empty() {
                        continue;
                    }

                    let identify_start = std::time::Instant::now();
                    if let Err(e) = identifier.identify_contracts(&contract_addresses).await {
                        tracing::warn!(
                            target: "torii::etl",
                            error = %e,
                            "Contract identification failed"
                        );
                    }
                    ::metrics::histogram!("torii_registry_identify_duration_seconds")
                        .record(identify_start.elapsed().as_secs_f64());
                }
            });
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        let producer_extractor = extractor.clone();
        let producer_engine_db = etl_engine_db.clone();
        let producer_shutdown = etl_shutdown_token.clone();
        let producer_identify_tx = identify_tx.clone();
        let producer_queue_depth = queue_depth.clone();

        let producer_handle = tokio::spawn(async move {
            let mut cursor: Option<String> = None;

            loop {
                if producer_shutdown.is_cancelled() {
                    tracing::info!(target: "torii::etl", "Shutdown requested, stopping prefetch producer");
                    break;
                }

                let batch = {
                    let mut extractor = producer_extractor.lock().await;
                    extractor.extract(cursor.clone(), &producer_engine_db).await
                };

                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        tracing::error!(target: "torii::etl", "Extract failed: {}", e);
                        ::metrics::counter!("torii_etl_cycle_total", "status" => "extract_error")
                            .increment(1);
                        if producer_shutdown.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(cycle_interval)).await;
                        continue;
                    }
                };

                let should_pause = batch.is_empty();
                let new_cursor = batch.cursor.clone();

                if let Some(ref identify_tx) = producer_identify_tx {
                    let contract_addresses: Vec<starknet::core::types::Felt> = batch
                        .events
                        .iter()
                        .map(|event| event.from_address)
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .collect();

                    if !contract_addresses.is_empty() {
                        match identify_tx.try_send(contract_addresses) {
                            Ok(()) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "enqueued"
                                )
                                .increment(1);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "dropped"
                                )
                                .increment(1);
                                tracing::debug!(
                                    target: "torii::etl",
                                    "Contract identify queue full, skipping identify for this batch"
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "closed"
                                )
                                .increment(1);
                            }
                        }
                    }
                }

                let extractor_finished = {
                    let extractor = producer_extractor.lock().await;
                    extractor.is_finished()
                };

                let stall_start = std::time::Instant::now();
                if prefetch_tx
                    .send(PrefetchedBatch {
                        batch,
                        cursor: new_cursor.clone(),
                        extractor_finished,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
                ::metrics::histogram!("torii_etl_prefetch_stall_seconds")
                    .record(stall_start.elapsed().as_secs_f64());
                producer_queue_depth.fetch_add(1, Ordering::Relaxed);
                ::metrics::gauge!("torii_etl_prefetch_queue_depth")
                    .set(producer_queue_depth.load(Ordering::Relaxed) as f64);

                cursor = new_cursor;

                if extractor_finished {
                    break;
                }

                if let Some(prefetched_cursor) = &cursor {
                    tracing::trace!(
                        target: "torii::etl",
                        cursor = prefetched_cursor,
                        "Prefetched ETL batch"
                    );
                }

                if prefetch_tx.is_closed() || producer_shutdown.is_cancelled() {
                    break;
                }

                if let Some(ref prefetched_cursor) = cursor {
                    tracing::trace!(
                        target: "torii::etl",
                        cursor = prefetched_cursor,
                        "Advanced producer cursor"
                    );
                }

                if should_pause {
                    tokio::time::sleep(tokio::time::Duration::from_secs(cycle_interval)).await;
                }
            }
        });

        let mut committed_cursor: Option<String> = None;
        let mut shutdown_requested = false;

        loop {
            ::metrics::gauge!("torii_etl_inflight_cycles").set(1.0);

            let wait_start = std::time::Instant::now();
            let next_batch = if shutdown_requested {
                prefetch_rx.recv().await
            } else {
                tokio::select! {
                    maybe_batch = prefetch_rx.recv() => maybe_batch,
                    () = etl_shutdown_token.cancelled() => {
                        shutdown_requested = true;
                        continue;
                    }
                }
            };
            ::metrics::histogram!("torii_etl_prefetch_stall_seconds")
                .record(wait_start.elapsed().as_secs_f64());

            let Some(prefetched) = next_batch else {
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                break;
            };
            queue_depth.fetch_sub(1, Ordering::Relaxed);
            ::metrics::gauge!("torii_etl_prefetch_queue_depth")
                .set(queue_depth.load(Ordering::Relaxed) as f64);

            let cycle_start = std::time::Instant::now();
            let batch = prefetched.batch;
            let new_cursor = prefetched.cursor;

            if batch.is_empty() {
                if let Some(ref cursor_str) = new_cursor {
                    if committed_cursor.as_ref() != Some(cursor_str) {
                        let commit_result = {
                            let mut extractor = extractor.lock().await;
                            extractor.commit_cursor(cursor_str, &etl_engine_db).await
                        };
                        if let Err(e) = commit_result {
                            tracing::error!(
                                target: "torii::etl",
                                "Failed to commit cursor for empty batch: {}",
                                e
                            );
                            ::metrics::counter!("torii_cursor_commit_failures_total").increment(1);
                        } else {
                            committed_cursor.clone_from(&new_cursor);
                        }
                    }
                }

                if prefetched.extractor_finished {
                    tracing::info!(target: "torii::etl", "Extractor finished, stopping ETL loop");
                    ::metrics::counter!("torii_etl_cycle_total", "status" => "empty").increment(1);
                    ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                        .record(cycle_start.elapsed().as_secs_f64());
                    ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                    break;
                }

                ::metrics::counter!("torii_etl_cycle_total", "status" => "empty").increment(1);
                ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                    .record(cycle_start.elapsed().as_secs_f64());
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);

                if shutdown_requested {
                    continue;
                }
                continue;
            }

            tracing::info!(
                target: "torii::etl",
                "Extracted {} events",
                batch.len()
            );
            ::metrics::counter!("torii_events_extracted_total").increment(batch.len() as u64);
            ::metrics::counter!("torii_tx_processed_total")
                .increment(batch.transactions.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "events")
                .increment(batch.events.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "blocks")
                .increment(batch.blocks.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "transactions")
                .increment(batch.transactions.len() as u64);

            // Update the engine DB stats for now here. Temporary.
            let latest_block = batch.blocks.keys().max().copied().unwrap_or(0);
            if let Err(e) = etl_engine_db
                .update_head(latest_block, batch.len() as u64)
                .await
            {
                tracing::warn!(target: "torii::etl", "Failed to update engine DB: {}", e);
            }

            // Transform the events into envelopes.
            let envelopes = match etl_decoder_context.decode(&batch.events).await {
                Ok(envelopes) => envelopes,
                Err(e) => {
                    tracing::error!(target: "torii::etl", "Decode failed: {}", e);
                    ::metrics::counter!("torii_decode_failures_total", "stage" => "decode")
                        .increment(1);
                    ::metrics::counter!("torii_etl_cycle_total", "status" => "decode_error")
                        .increment(1);
                    ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                        .record(cycle_start.elapsed().as_secs_f64());
                    ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                    continue;
                }
            };
            ::metrics::counter!("torii_events_decoded_total").increment(batch.events.len() as u64);
            ::metrics::counter!("torii_decode_envelopes_total").increment(envelopes.len() as u64);

            // Load the envelopes into the sinks.
            if let Err(e) = etl_multi_sink.process(&envelopes, &batch).await {
                tracing::error!(target: "torii::etl", "Sink processing failed: {}", e);
                ::metrics::counter!("torii_etl_cycle_total", "status" => "sink_error").increment(1);
                ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                    .record(cycle_start.elapsed().as_secs_f64());
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                continue;
            }

            // Count successfully processed payloads (post-sink processing).
            ::metrics::counter!("torii_events_processed_total")
                .increment(batch.events.len() as u64);
            ::metrics::counter!("torii_transactions_processed_total")
                .increment(batch.transactions.len() as u64);

            // CRITICAL: Commit cursor ONLY AFTER successful sink processing.
            // This ensures no data loss if the process is killed during extraction or sink processing.
            if let Some(ref cursor_str) = new_cursor {
                let commit_result = {
                    let mut extractor = extractor.lock().await;
                    extractor.commit_cursor(cursor_str, &etl_engine_db).await
                };
                if let Err(e) = commit_result {
                    tracing::error!(target: "torii::etl", "Failed to commit cursor: {}", e);
                    ::metrics::counter!("torii_cursor_commit_failures_total").increment(1);
                    // Continue anyway - cursor will be re-processed on restart (safe, just duplicate work)
                } else {
                    committed_cursor.clone_from(&new_cursor);
                }
            }

            if let Some(chain_head) = batch.chain_head {
                let gap = chain_head.saturating_sub(latest_block);
                ::metrics::gauge!("torii_etl_cycle_gap_blocks").set(gap as f64);
            }
            ::metrics::gauge!("torii_etl_last_success_timestamp_seconds")
                .set(chrono::Utc::now().timestamp() as f64);
            ::metrics::counter!("torii_etl_cycle_total", "status" => "ok").increment(1);
            ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                .record(cycle_start.elapsed().as_secs_f64());
            ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);

            tracing::info!(target: "torii::etl", "ETL cycle complete");
        }

        if let Err(e) = producer_handle.await {
            tracing::warn!(target: "torii::etl", error = %e, "Prefetch producer join failed");
        }
        drop(identify_tx);
        if let Some(handle) = identify_handle {
            if let Err(e) = handle.await {
                tracing::warn!(target: "torii::etl", error = %e, "Identifier worker join failed");
            }
        }
        tracing::info!(target: "torii::etl", "ETL loop completed gracefully");
    });

    // Setup signal handlers for graceful shutdown
    let server_shutdown_token = shutdown_token.clone();
    let shutdown_timeout = config.shutdown_timeout;

    let shutdown_signal = async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {
                tracing::info!(target: "torii::main", "Received SIGINT (Ctrl+C), initiating graceful shutdown...");
            }
            () = terminate => {
                tracing::info!(target: "torii::main", "Received SIGTERM, initiating graceful shutdown...");
            }
        }

        // Signal shutdown to ETL loop
        server_shutdown_token.cancel();
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal);

    // Give active connections 15 seconds to close gracefully, then force shutdown.
    // This prevents hanging on long-lived gRPC streaming connections.
    const SERVER_SHUTDOWN_TIMEOUT_SECS: u64 = 15;
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(target: "torii::main", "Server error: {}", e);
            }
        }
        () = async {
            // Wait for shutdown signal + timeout
            shutdown_token.cancelled().await;
            tokio::time::sleep(Duration::from_secs(SERVER_SHUTDOWN_TIMEOUT_SECS)).await;
        } => {
            tracing::warn!(
                target: "torii::main",
                "Server connections did not close within {}s, forcing shutdown",
                SERVER_SHUTDOWN_TIMEOUT_SECS
            );
        }
    }

    tracing::info!(target: "torii::main", "HTTP/gRPC server stopped, waiting for ETL loop to complete...");

    // Wait for ETL loop to finish with timeout
    match tokio::time::timeout(Duration::from_secs(shutdown_timeout), etl_handle).await {
        Ok(Ok(())) => {
            tracing::info!(target: "torii::main", "ETL loop completed successfully");
        }
        Ok(Err(e)) => {
            tracing::error!(target: "torii::main", "ETL loop panicked: {}", e);
        }
        Err(_) => {
            tracing::warn!(
                target: "torii::main",
                "ETL loop did not complete within {}s timeout, forcing shutdown",
                shutdown_timeout
            );
        }
    }

    tracing::info!(target: "torii::main", "Torii shutdown complete");

    Ok(())
}
