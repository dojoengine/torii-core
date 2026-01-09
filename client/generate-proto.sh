#!/bin/bash

set -e

# Makes sure the proto files are up to date.
cp ../proto/torii.proto ./proto/torii.proto
cp ../crates/torii-sql-sink/proto/sql.proto ./proto/sinks/sql.proto
cp ../crates/torii-log-sink/proto/log.proto ./proto/sinks/log.proto


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/proto"
PROTO_DIR_ROOT="$SCRIPT_DIR/../proto"
OUT_DIR="$SCRIPT_DIR/src/generated"

echo "Creating output directory..."
mkdir -p "$OUT_DIR"

# Check if protoc is available
if ! command -v protoc &>/dev/null; then
  echo "Error: protoc is not installed. Please install Protocol Buffers compiler."
  echo "On macOS: brew install protobuf"
  exit 1
fi

# Find the protobuf-ts plugin
PROTOC_GEN_TS="$SCRIPT_DIR/node_modules/.bin/protoc-gen-ts"

if [ ! -f "$PROTOC_GEN_TS" ]; then
  echo "Error: @protobuf-ts/plugin not found. Run 'pnpm install' first."
  exit 1
fi

# Generate TypeScript code with protobuf-ts
echo "Generating TypeScript protobuf code..."

# Generate for main proto (torii.proto) from client proto directory
protoc \
  --experimental_allow_proto3_optional \
  --proto_path="$PROTO_DIR" \
  --plugin="protoc-gen-ts=$PROTOC_GEN_TS" \
  --ts_out="$OUT_DIR" \
  --ts_opt=generate_dependencies \
  --ts_opt=output_typescript \
  --ts_opt=use_proto_field_name \
  --ts_opt=client_generic \
  "$PROTO_DIR"/torii.proto

# Then, generate for sink protos (sql.proto, etc.) from client proto directory
echo "Generating sink protobuf code..."
protoc \
  --experimental_allow_proto3_optional \
  --proto_path="$PROTO_DIR" \
  --plugin="protoc-gen-ts=$PROTOC_GEN_TS" \
  --ts_out="$OUT_DIR" \
  --ts_opt=generate_dependencies \
  --ts_opt=output_typescript \
  --ts_opt=use_proto_field_name \
  --ts_opt=client_generic \
  "$PROTO_DIR"/sinks/*.proto

echo "✅ Proto compilation complete!"
echo "Generated files in: $OUT_DIR"
ls -la "$OUT_DIR"
