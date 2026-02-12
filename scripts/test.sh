#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Running Rust Tests ===${NC}"

# Run tests
echo -e "\n${YELLOW}Running tests...${NC}"
if cargo test --workspace "$@"; then
    echo -e "\n${GREEN}✓ All tests passed!${NC}"
else
    echo -e "\n${RED}✗ Tests failed${NC}"
    exit 1
fi
