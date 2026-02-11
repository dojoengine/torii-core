# Block Extractor Test Example

Simple standalone example to test the `BlockRangeExtractor` without sinks or decoders.

## Quick Start

### Test with local Katana node

```bash
# Start Katana (Starknet devnet)
katana

# Run the example (defaults to localhost:5050)
cargo run --example test_block_extractor
```

### Test with public RPC

```bash
# Starknet Sepolia testnet
STARKNET_RPC_URL="https://starknet-sepolia.public.blastapi.io/rpc/v0_7" \
FROM_BLOCK=100000 \
TO_BLOCK=100005 \
cargo run --example test_block_extractor

# Starknet mainnet (be mindful of rate limits on public RPCs)
STARKNET_RPC_URL="https://starknet-mainnet.public.blastapi.io/rpc/v0_7" \
FROM_BLOCK=600000 \
TO_BLOCK=600002 \
cargo run --example test_block_extractor
```

## Environment Variables

- `STARKNET_RPC_URL` - RPC endpoint (default: `http://localhost:5050`)
- `FROM_BLOCK` - Starting block number (default: `600000`)
- `TO_BLOCK` - Ending block number (default: `FROM_BLOCK + 2`)
- `RUST_LOG` - Log level (default: `torii=debug,test_block_extractor=info`)

## What It Tests

1. **Provider setup** - Creates a JSON-RPC provider
2. **Extractor creation** - Initializes `BlockRangeExtractor`
3. **Block extraction** - Fetches blocks with receipts using batch requests
4. **Event extraction** - Extracts all events from transaction receipts
5. **Context building** - Builds deduplicated block/transaction context
6. **Cursor persistence** - Saves cursor to `EngineDb`
7. **Resume from cursor** - Tests resuming extraction from saved state

## Expected Output

```
🧪 Testing BlockRangeExtractor

✅ Created in-memory EngineDb
📡 RPC URL: http://localhost:5050
📦 Extracting blocks 600000-600002

✅ Created BlockRangeExtractor

🔄 Starting extraction...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 EXTRACTION RESULTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📦 Blocks extracted: 3
📝 Events extracted: 42
💳 Transactions: 18
🔖 Cursor: Some("block:600002")
➡️  Has more: false

📦 Block Details:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Block #600000: hash=0x..., timestamp=1234567890
  Block #600001: hash=0x..., timestamp=1234567891
  Block #600002: hash=0x..., timestamp=1234567892

📝 Event Summary:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Block #600000: 15 events
  Block #600001: 12 events
  Block #600002: 15 events

  First few events:
    Event #1
      From: 0x1234...
      Keys: 2 key(s)
      Data: 3 field(s)
      Block: Some(600000)
    ...

🔖 Testing cursor persistence...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ Cursor saved to EngineDb: 600002

♻️  Testing resume from cursor...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ℹ️  No more data to extract (reached end block)

✅ Test completed successfully!
```

## Next Steps

After verifying the extractor works:

1. **Add unit tests** with mocked `EngineDb` and `Provider`
2. **Integrate with examples** - Replace `SampleExtractor` in existing examples
3. **Performance testing** - Test with larger block ranges and different batch sizes
4. **Error handling** - Test network failures, invalid blocks, etc.
