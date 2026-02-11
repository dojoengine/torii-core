#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
FIX_MODE=false
if [[ "${1:-}" == "--fix" ]]; then
    FIX_MODE=true
    echo -e "${YELLOW}=== Running Rust Linting (FIX MODE) ===${NC}"
else
    echo -e "${YELLOW}=== Running Rust Linting (CHECK MODE) ===${NC}"
fi

# 1. Format check/fix
if [ "$FIX_MODE" = true ]; then
    echo -e "\n${YELLOW}[1/2] Formatting code (cargo fmt)...${NC}"
    if cargo fmt --all; then
        echo -e "${GREEN}✓ Code formatted${NC}"
    else
        echo -e "${RED}✗ Formatting failed${NC}"
        exit 1
    fi
else
    echo -e "\n${YELLOW}[1/2] Checking formatting (cargo fmt)...${NC}"
    if cargo fmt --all -- --check; then
        echo -e "${GREEN}✓ Formatting OK${NC}"
    else
        echo -e "${RED}✗ Formatting issues found. Run './scripts/lint.sh --fix' to fix.${NC}"
        exit 1
    fi
fi

# 2. Clippy check/fix
if [ "$FIX_MODE" = true ]; then
    echo -e "\n${YELLOW}[2/2] Running clippy with fixes...${NC}"
    if cargo clippy --workspace --all-targets --all-features --fix --allow-dirty --allow-staged; then
        echo -e "${GREEN}✓ Clippy fixes applied${NC}"
    else
        echo -e "${RED}✗ Clippy found issues that couldn't be automatically fixed${NC}"
        echo -e "${YELLOW}Run 'cargo clippy --workspace --all-targets --all-features' to see remaining issues${NC}"
        exit 1
    fi
else
    echo -e "\n${YELLOW}[2/2] Running clippy...${NC}"
    if cargo clippy --workspace --all-targets --all-features -- -D warnings; then
        echo -e "${GREEN}✓ Clippy OK${NC}"
    else
        echo -e "${RED}✗ Clippy found issues. Try './scripts/lint.sh --fix' to auto-fix some issues.${NC}"
        exit 1
    fi
fi

echo -e "\n${GREEN}=== All lint checks passed! ===${NC}"
