#!/usr/bin/env bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${YELLOW}Setting up git hooks...${NC}"

# Configure git to use .githooks directory
git config core.hooksPath .githooks

# Make hooks executable
chmod +x "$REPO_ROOT/.githooks/"*

echo -e "${GREEN}✓ Git hooks installed successfully!${NC}"
echo ""
echo "Hooks enabled:"
echo "  - pre-commit: Runs formatting and clippy checks"
echo "  - pre-push: Runs full lint script (fmt + clippy + tests)"
echo ""
echo "To disable hooks temporarily, use: git commit --no-verify"
