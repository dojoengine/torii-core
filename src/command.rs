use anyhow::Result;
use async_trait::async_trait;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle, JoinSet};

use crate::etl::sink::EventBus;

pub trait Command: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<T> Command for T
where
    T: Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

#[async_trait]
pub trait CommandHandler: Send + Sync {
    fn supports(&self, command: &dyn Command) -> bool;

    fn attach_event_bus(&self, _event_bus: Arc<EventBus>) {}

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()>;
}

struct RoutedCommand {
    handler_index: usize,
    command: Box<dyn Command>,
    command_name: &'static str,
}

#[derive(Debug)]
pub enum CommandDispatchError {
    Unsupported(&'static str),
    Ambiguous(&'static str),
    QueueFull(&'static str),
    Closed(&'static str),
}

impl fmt::Display for CommandDispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(name) => write!(f, "no command handler is registered for {name}"),
            Self::Ambiguous(name) => write!(f, "multiple command handlers matched {name}"),
            Self::QueueFull(name) => write!(f, "command queue is full for {name}"),
            Self::Closed(name) => write!(f, "command queue is closed for {name}"),
        }
    }
}

impl std::error::Error for CommandDispatchError {}

#[derive(Clone)]
pub struct CommandBusSender {
    tx: mpsc::Sender<RoutedCommand>,
    handlers: Arc<Vec<Arc<dyn CommandHandler>>>,
}

impl CommandBusSender {
    pub fn dispatch<C>(&self, command: C) -> std::result::Result<(), CommandDispatchError>
    where
        C: Command,
    {
        let command: Box<dyn Command> = Box::new(command);
        let command_name = command.name();

        let mut matches = self
            .handlers
            .iter()
            .enumerate()
            .filter(|(_, handler)| handler.supports(command.as_ref()));

        let Some((handler_index, _)) = matches.next() else {
            ::metrics::counter!(
                "torii_command_dispatch_total",
                "command" => command_name,
                "status" => "unsupported"
            )
            .increment(1);
            return Err(CommandDispatchError::Unsupported(command_name));
        };

        if matches.next().is_some() {
            ::metrics::counter!(
                "torii_command_dispatch_total",
                "command" => command_name,
                "status" => "ambiguous"
            )
            .increment(1);
            return Err(CommandDispatchError::Ambiguous(command_name));
        }

        let routed = RoutedCommand {
            handler_index,
            command,
            command_name,
        };

        match self.tx.try_send(routed) {
            Ok(()) => {
                ::metrics::counter!(
                    "torii_command_dispatch_total",
                    "command" => command_name,
                    "status" => "enqueued"
                )
                .increment(1);
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                ::metrics::counter!(
                    "torii_command_dispatch_total",
                    "command" => command_name,
                    "status" => "dropped_full"
                )
                .increment(1);
                Err(CommandDispatchError::QueueFull(command_name))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                ::metrics::counter!(
                    "torii_command_dispatch_total",
                    "command" => command_name,
                    "status" => "closed"
                )
                .increment(1);
                Err(CommandDispatchError::Closed(command_name))
            }
        }
    }
}

pub struct CommandBus {
    sender: CommandBusSender,
    receiver_handle: JoinHandle<()>,
}

impl CommandBus {
    pub fn new(handlers: Vec<Box<dyn CommandHandler>>, queue_size: usize) -> Result<Self> {
        let handlers: Arc<Vec<Arc<dyn CommandHandler>>> =
            Arc::new(handlers.into_iter().map(Arc::from).collect());

        let (tx, mut rx) = mpsc::channel::<RoutedCommand>(queue_size.max(1));
        let sender = CommandBusSender {
            tx,
            handlers: handlers.clone(),
        };

        let receiver_handle = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut receiver_closed = false;

            loop {
                tokio::select! {
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                        if let Err(error) = result {
                            log_join_error(error);
                        }
                    }
                    maybe_command = rx.recv(), if !receiver_closed => {
                        match maybe_command {
                            Some(routed) => {
                                let Some(handler) = handlers.get(routed.handler_index).cloned() else {
                                    tracing::error!(
                                        target: "torii::command_bus",
                                        command = routed.command_name,
                                        handler_index = routed.handler_index,
                                        "Resolved command handler is missing"
                                    );
                                    continue;
                                };

                                tasks.spawn(async move {
                                    let start = std::time::Instant::now();
                                    let result = handler.handle_command(routed.command).await;
                                    ::metrics::histogram!(
                                        "torii_command_handle_duration_seconds",
                                        "command" => routed.command_name
                                    )
                                    .record(start.elapsed().as_secs_f64());

                                    match result {
                                        Ok(()) => {
                                            ::metrics::counter!(
                                                "torii_command_handle_total",
                                                "command" => routed.command_name,
                                                "status" => "ok"
                                            )
                                            .increment(1);
                                        }
                                        Err(error) => {
                                            ::metrics::counter!(
                                                "torii_command_handle_total",
                                                "command" => routed.command_name,
                                                "status" => "error"
                                            )
                                            .increment(1);
                                            tracing::warn!(
                                                target: "torii::command_bus",
                                                command = routed.command_name,
                                                error = %error,
                                                "Command handler failed"
                                            );
                                        }
                                    }
                                });
                            }
                            None => receiver_closed = true,
                        }
                    }
                    else => break,
                }

                if receiver_closed && tasks.is_empty() {
                    break;
                }
            }
        });

        Ok(Self {
            sender,
            receiver_handle,
        })
    }

    pub fn sender(&self) -> CommandBusSender {
        self.sender.clone()
    }

    pub async fn shutdown(self) {
        self.receiver_handle.abort();
        match self.receiver_handle.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {
                tracing::info!(target: "torii::command_bus", "Command bus stopped");
            }
            Err(error) => {
                tracing::warn!(
                    target: "torii::command_bus",
                    error = %error,
                    "Command bus shutdown join failed"
                );
            }
        }
    }
}

fn log_join_error(error: JoinError) {
    if error.is_cancelled() {
        tracing::debug!(target: "torii::command_bus", "Command task cancelled");
    } else {
        tracing::warn!(
            target: "torii::command_bus",
            error = %error,
            "Command task join failed"
        );
    }
}
