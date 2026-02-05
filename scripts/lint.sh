#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Running Rust Code Quality Checks ===${NC}"

# 1. Format check
echo -e "\n${YELLOW}[1/3] Checking formatting (cargo fmt)...${NC}"
if cargo fmt --all -- --check; then
    echo -e "${GREEN}✓ Formatting OK${NC}"
else
    echo -e "${RED}✗ Formatting issues found. Run 'cargo fmt --all' to fix.${NC}"
    exit 1
fi

# 2. Clippy
echo -e "\n${YELLOW}[2/3] Running clippy...${NC}"
if cargo clippy --workspace --all-targets --all-features -- -D warnings; then
    echo -e "${GREEN}✓ Clippy OK${NC}"
else
    echo -e "${RED}✗ Clippy found issues${NC}"
    exit 1
fi

# 3. Tests
echo -e "\n${YELLOW}[3/3] Running tests...${NC}"
if cargo test --workspace; then
    echo -e "${GREEN}✓ Tests OK${NC}"
else
    echo -e "${RED}✗ Tests failed${NC}"
    exit 1
fi

echo -e "\n${GREEN}=== All checks passed! ===${NC}"
