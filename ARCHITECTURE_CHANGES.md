# Torii Architecture Refactoring Summary

## Overview

This document summarizes the major architectural changes made to Torii to support a cleaner, more robust library API.

## Key Changes

### 1. ToriiConfig Extensions

**Before**: Limited configuration options
```rust
ToriiConfig::builder()
    .add_sink_boxed(sink)
    .add_decoder(decoder)
    .build()
```

**After**: Comprehensive configuration
```rust
ToriiConfig::builder()
    .port(3000)
    .database_root("./data")                    // NEW: Database co-location
    .with_extractor(extractor)                  // NEW: Pluggable extractor
    .identification_mode(ContractIdentificationMode::SRC5)  // NEW: Contract ID mode
    .map_contract(addr, vec![DecoderId::new("erc20")])      // NEW: Explicit mappings
    .with_identification_rule(Box::new(Erc20Rule))          // NEW: Auto-discovery rules
    .add_decoder(decoder)
    .add_sink_boxed(sink)
    .build()
```

### 2. Sink Initialization Context

**Before**: Only EventBus
```rust
async fn initialize(&mut self, event_bus: Arc<EventBus>) -> Result<()>
```

**After**: EventBus + SinkContext
```rust
async fn initialize(
    &mut self,
    event_bus: Arc<EventBus>,
    context: &SinkContext,  // NEW: Extensible context
) -> Result<()>
```

**SinkContext provides**:
- `database_root`: Path where sinks should create databases
- Future: metrics, config, shared resources

### 3. ContractRegistry Integration

**Before**: Manual creation and passing
```rust
let registry = ContractRegistry::new(...);
let decoder = MultiDecoder::with_registry(decoders, registry);
```

**After**: Created internally by `run()`
```rust
// User just configures through ToriiConfig
ToriiConfig::builder()
    .identification_mode(ContractIdentificationMode::SRC5)
    .map_contract(addr, decoders)
    .with_identification_rule(rule)
    .build()

// run() creates registry internally
torii::run(config).await?;
```

### 4. Extractor Configuration

**Before**: Only SampleExtractor for testing
```rust
// Hardcoded in run() function
let extractor = SampleExtractor::new(sample_events);
```

**After**: Pluggable via ToriiConfig
```rust
let extractor = Box::new(BlockRangeExtractor::new(provider, config));

ToriiConfig::builder()
    .with_extractor(extractor)  // Any Extractor implementation
    .build()
```

### 5. Database Co-location

**Before**: Each component manages its own database path
```rust
let engine_db = EngineDb::new(":memory:".to_string())?;
let sink_db = SqliteSink::new("./my-sink.db")?;
```

**After**: All databases in configured root
```rust
ToriiConfig::builder()
    .database_root("./data")  // All DBs go here
    .build()

// Torii creates:
// - ./data/engine.db (ETL state)
// - Sinks use context.database_root to create their DBs
```

### 6. ETL Loop Improvements

**Before**: Sample-only with hardcoded interval
```rust
let mut interval = tokio::time::interval(Duration::from_secs(3));
loop {
    interval.tick().await;
    let batch = extractor.extract(None, &engine_db).await?;
    // ...
}
```

**After**: Proper cursor management + configured interval
```rust
let cycle_interval = config.cycle_interval;
let mut cursor: Option<String> = None;

loop {
    let batch = extractor.extract(cursor.clone(), &engine_db).await?;
    cursor = batch.cursor.clone();  // Persist cursor

    if batch.is_empty() {
        if extractor.is_finished() { break; }
        tokio::time::sleep(Duration::from_secs(cycle_interval)).await;
        continue;
    }
    // ... process
}
```

## Usage Pattern

### Before (manual ETL loop)

```rust
// User had to implement entire ETL loop
let provider = ...;
let extractor = BlockRangeExtractor::new(provider, config);
let registry = ContractRegistry::new(...);
let decoder = MultiDecoder::with_registry(decoders, registry);
let mut sink = MySink::new();
sink.initialize(event_bus).await?;

loop {
    let batch = extractor.extract(cursor, &engine_db).await?;
    let envelopes = decoder.decode(&batch.events).await?;
    sink.process(&envelopes, &batch).await?;
}
```

### After (clean library API)

```rust
// User just configures and calls run()
let extractor = Box::new(BlockRangeExtractor::new(provider, config));
let decoder = Arc::new(MyDecoder::new());
let sink = Box::new(MySink::new(storage));

let config = ToriiConfig::builder()
    .database_root("./data")
    .with_extractor(extractor)
    .add_decoder(decoder)
    .add_sink_boxed(sink)
    .identification_mode(ContractIdentificationMode::SRC5)
    .map_contract(eth_address, vec![DecoderId::new("erc20")])
    .with_identification_rule(Box::new(Erc20Rule))
    .build();

torii::run(config).await?;  // Done!
```

## Reference Implementation

See `bins/torii-erc20/` for a complete reference implementation showing:
- BlockRangeExtractor configuration
- Contract identification (explicit + auto-discovery)
- Custom decoder and sink
- Clean ~160 line main.rs using torii::run()

## Migration Guide

### For Sink Developers

1. **Update initialize() signature**:
   ```rust
   async fn initialize(
       &mut self,
       event_bus: Arc<EventBus>,
       context: &torii::etl::sink::SinkContext,  // Add this
   ) -> Result<()>
   ```

2. **Use database_root for co-location**:
   ```rust
   async fn initialize(&mut self, event_bus: Arc<EventBus>, context: &SinkContext) -> Result<()> {
       let db_path = context.database_root.join("my-sink.db");
       self.db = Database::open(db_path)?;
       Ok(())
   }
   ```

### For Application Developers

1. **Create extractor explicitly**:
   ```rust
   let extractor = Box::new(BlockRangeExtractor::new(provider, config));
   ```

2. **Configure ToriiConfig with new fields**:
   ```rust
   ToriiConfig::builder()
       .with_extractor(extractor)
       .database_root("./data")
       .identification_mode(ContractIdentificationMode::SRC5)
       .map_contract(addr, decoders)
       .with_identification_rule(rule)
       .build()
   ```

3. **Call torii::run() instead of manual loop**:
   ```rust
   torii::run(config).await?;
   ```

## Benefits

1. **Cleaner API**: Configure once, call run() - no manual ETL loop
2. **Database Co-location**: All databases in one root for easy backup
3. **Pluggable Extraction**: Any Extractor implementation, not just samples
4. **Contract Identification**: Integrated into core, not manual
5. **Extensible Context**: SinkContext can grow without breaking API
6. **Proper State Management**: Cursor persistence, finished detection
7. **Reference Implementation**: torii-erc20 shows best practices

## Future Improvements

- [ ] Persist ContractRegistry cache to EngineDb
- [ ] Add ABI cache to EngineDb (class hash → ABI mapping)
- [ ] Metrics in SinkContext
- [ ] Shared configuration in SinkContext
- [ ] Parallel sink processing
- [ ] Graceful shutdown handling
