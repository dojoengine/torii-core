use anyhow::{Context, Result};
use std::sync::Arc;
use tokio_postgres::NoTls;
use torii::command::{CommandBus, CommandHandler};
use torii::etl::sink::{EventBus, Sink, SinkContext};
use torii::grpc::SubscriptionManager;

pub struct InitializedSinkRuntime {
    command_bus: CommandBus,
}

impl InitializedSinkRuntime {
    pub async fn shutdown(self) {
        self.command_bus.shutdown().await;
    }
}

pub async fn initialize_sink(sink: &mut dyn Sink, database_root: std::path::PathBuf) -> Result<()> {
    let runtime = initialize_sink_with_command_handlers(sink, database_root, Vec::new(), 1).await?;
    std::mem::drop(runtime);
    Ok(())
}

pub async fn initialize_sink_with_command_handlers(
    sink: &mut dyn Sink,
    database_root: std::path::PathBuf,
    handlers: Vec<Box<dyn CommandHandler>>,
    queue_size: usize,
) -> Result<InitializedSinkRuntime> {
    let event_bus = Arc::new(EventBus::new(Arc::new(SubscriptionManager::new())));
    for handler in &handlers {
        handler.attach_event_bus(event_bus.clone());
    }
    let command_bus = CommandBus::new(handlers, queue_size)?;
    sink.initialize(
        event_bus,
        &SinkContext {
            database_root,
            command_bus: command_bus.sender(),
        },
    )
    .await?;
    Ok(InitializedSinkRuntime { command_bus })
}

pub async fn drop_postgres_schemas(
    database_url: &str,
    schemas: &[&str],
    logging_target: &str,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .context("failed to connect for schema reset")?;

    let requested_target = logging_target.to_string();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(
                target: "torii_runtime_common::sink",
                error = %e,
                requested_target,
                "postgres reset connection failed"
            );
        }
    });

    for schema in schemas {
        let query = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
        client
            .batch_execute(&query)
            .await
            .with_context(|| format!("failed to drop schema {schema}"))?;
    }

    Ok(())
}
