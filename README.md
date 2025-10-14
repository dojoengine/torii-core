# Torii Runtime Workspace

Torii indexes Starknet by turning raw on-chain events into canonical domain events and fanning
those out to backend-specific sinks. The workspace is organised around a few core ideas:

- **Fetchers** pull raw Starknet events. Each fetcher crate (starting with
  `torii-fetcher-jsonrpc`) knows how to talk to a data source and expose events through the
  `Fetcher` trait.
- **Decoders** translate `EmittedEvent` structures into strongly typed `Envelope`s. Every
  protocol (Introspect, ERC20, ERC721, …) ships its own decoder crate plus a serialisable
  configuration describing which selectors to watch and **one or more** contract addresses to
  match.
- **Sinks** consume batches of envelopes and apply them to a backend (logging, SQLite, Postgres,
  gRPC, …). Sinks declare their own config and can downcast envelopes to the event types they
  understand.
- **Registries** glue these pieces together. `DecoderRegistry`, `SinkRegistry`, and the new
  `fetchers` helper live in `torii-registry`, which can build components directly from
  configuration maps so binaries stay slim.

A high-level flow:

1. Load configuration (TOML → `HashMap<String, Value>` per section).
2. Use `torii-registry::fetchers::from_config` to build the requested fetcher (only one at a time).
3. Use `torii-registry::decoders::from_config` to register the decoders that should be active.
4. Use `torii-registry::sinks::from_config` (passing both sink and contract maps) to instantiate the sinks that should receive batches.
5. Run the batch loop: fetch events, decode via the registry, deliver batches to each sink.

```
crates/
  core/
    torii-core/         # runtime traits & batch driver
    torii-registry/     # helpers to build registries from config maps
  decoders/
    torii-decoder-*     # protocol-specific decoders + configs
  fetchers/
    torii-fetcher-*     # data-source fetchers + configs
  sinks/
    torii-sink-*        # backend sinks + configs
  types/
    torii-types-*       # canonical payload structs & type IDs
bins/
  log-sink-only/        # synthetic events -> log sink
  sqlite-sinks/         # config-driven example wiring log + sqlite sinks
config/
  sqlite-example.toml   # sample TOML used by the sqlite-sinks example
```

## Adding a Decoder

1. **Create the types** (if not already present) in `crates/types/torii-types-foo`, using
   `impl_event!(MyType, MY_TYPE_ID)` to implement the `Event` and `StaticEvent` traits. The type
   identifier is computed at compile-time from the type name string using Starknet's selector
   calculation, ensuring decoders and sinks share the same compact `FieldElement` identifier.
2. **List the type IDs in the decoder.** Implement the new `type_ids()` method on your decoder to
   return a static slice of the IDs it will emit. The registry will refuse to register decoders
   that collide, guaranteeing that sinks never see ambiguous events.
3. **Add a decoder crate** under `crates/decoders/torii-decoder-foo` that implements the
   `Decoder` trait, defines `FooDecoderConfig`, and exposes helper functions similar to the
   existing decoders.
4. **Wire it into torii-registry** by importing the crate in
   `crates/core/torii-registry/src/lib.rs` and extending the match inside
   `decoders::from_config` to handle `type = "foo"` entries.
5. **Register in the workspace** (`Cargo.toml`) and add the crate to any binary that needs the
   configuration helpers (usually by depending on `torii-registry`).
6. **Use it in config** via a `[decoders.foo]` section with the required fields. Decoders accept
   `contracts = ["0xdead", "0xbeef"]` so the same decoder can index multiple deployed contracts.

Binaries do not need manual code beyond reading the config; the registry helper handles the rest.

## Adding a Sink

1. **Implement the sink crate** under `crates/sinks/torii-sink-bar` with a config struct and a
   `Sink` implementation.
2. **Expose registration helpers** (`pub fn register(...)` or direct constructors) so the
   registry crate can instantiate it.
3. **Update torii-registry**: import the new sink crate and extend `sinks::from_config` with a
   branch for `type = "bar"`, creating the sink and pushing it into the registry.
4. **Add the crate to the workspace** and ensure any binary that uses it depends on
   `torii-registry` (which in turn pulls in the sink crate).
5. **Declare in config** via `[sinks.bar]` with the sink’s parameters.

## Adding a Fetcher

1. **Create the fetcher crate** under `crates/fetchers/torii-fetcher-foo`. Implement the
   `Fetcher` trait from `torii-core`, define a `FooFetcherConfig`, and expose a constructor like
   `FooFetcher::new(config) -> anyhow::Result<Self>`.
2. **Update torii-registry** by importing the crate in
   `crates/core/torii-registry/src/lib.rs` and extending `fetchers::from_config` with a branch for
   `type = "foo"`, returning an `Arc<dyn Fetcher>`.
3. **Add the crate to the workspace** and ensure any binary that uses it depends on
   `torii-registry`.
4. **Declare it in config** with a `[fetcher]` table. Only one fetcher is instantiated at a time.
   Example:

   ```toml
   [fetcher]
   type = "jsonrpc"
   rpc_url = "https://your.starknet.node"
   page_size = 512
   max_pages = 32
   address_backoff_ms = 50
   ```

   If a binary leaves `fetcher` undefined, it can fall back to a mock implementation (the
   `sqlite-sinks` example does this so it runs offline). The JSON-RPC fetcher iterates addresses
   sequentially; tweak `address_backoff_ms` to add a per-address pause when monitoring many
   contracts so you do not overwhelm shared RPC infrastructure.

## Configuration Flow

Binaries typically look like this:

```rust
let config = ...; // load TOML
let fetcher = torii_registry::fetchers::from_config(
    app.fetcher.as_ref().expect("fetcher config required"),
) .await?;
let decoder_registry = torii_registry::decoders::from_config(&app.decoders)?;
let sink_registry = torii_registry::sinks::from_config(&app.sinks, &app.contracts).await?;
let sinks = sink_registry.sinks().to_vec();
run_once_batch(fetcher.as_ref(), &decoder_registry, &sinks).await?;
```

If you need typed decoder configs (for mocks or fixtures), deserialise the TOML tables before
passing them to the registry helper.

## Examples

- `log-sink-only` shows manual wiring with synthetic events feeding the log sink.
- `sqlite-sinks` consumes `config/sqlite-example.toml`, dynamically enabling only the decoders
  and sinks declared in the file. If the `[fetcher]` section is omitted, it falls back to a mock
  fetcher so the example runs offline.

### Logging

All binaries install a compact `tracing` subscriber. `RUST_LOG` takes precedence; when it is absent
the default filter enables `info` level logs for the fetcher (`torii_fetcher_jsonrpc`), decoder
registration (`torii_decoders`), sink lifecycle (`torii_sinks`), the registry helpers, and the core
batch driver. Use `RUST_LOG=debug` or `=trace` for deeper visibility into fetch plans and envelope
fan-out.

Run them with:

```bash
cargo run -p log-sink-only
cargo run -p sqlite-sinks -- config/sqlite-example.toml
```

## Contributing

To test the full indexing pipeline with a configuration file, use the `simple-app` binary:

```bash
cargo run --bin simple-app config/sqlite-example.toml
```

For detailed logging output, set the `RUST_LOG` environment variable (optional):

```bash
RUST_LOG=trace cargo run --bin simple-app config/sqlite-example.toml
```
