#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SDK_DIR="${ROOT_DIR}/torii.js"
PACKAGE_JSON_PATH="${SDK_DIR}/package.json"
NPMRC_PATH="${SDK_DIR}/.npmrc.release"
PACKAGE_JSON_BACKUP="${SDK_DIR}/package.json.release-backup"

SKIP_GH_RELEASE=false
PACKAGE_OVERRIDE=""

usage() {
    echo "Usage: $0 [--skip-gh-release] [-p <package-name>|--package <package-name>]" >&2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-gh-release)
            SKIP_GH_RELEASE=true
            shift
            ;;
        -p|--package)
            if [[ $# -lt 2 ]]; then
                usage
                exit 1
            fi
            PACKAGE_OVERRIDE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

for tool in bun npm git node; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "$tool is required" >&2
        exit 1
    fi
done

if [[ "$SKIP_GH_RELEASE" != true ]] && ! command -v gh >/dev/null 2>&1; then
    echo "gh is required unless --skip-gh-release is used" >&2
    exit 1
fi

AUTH_TOKEN="${NODE_AUTH_TOKEN:-${NPM_TOKEN:-}}"
if [[ -z "$AUTH_TOKEN" ]]; then
    echo "NODE_AUTH_TOKEN or NPM_TOKEN is required" >&2
    exit 1
fi

if ! git -C "$ROOT_DIR" diff --quiet || ! git -C "$ROOT_DIR" diff --cached --quiet; then
    echo "Git working tree must be clean before publishing" >&2
    exit 1
fi

ORIGINAL_PACKAGE_NAME="$(node -p "require('${PACKAGE_JSON_PATH}').name")"
PACKAGE_NAME="${PACKAGE_OVERRIDE:-$ORIGINAL_PACKAGE_NAME}"
VERSION="$(node -p "require('${PACKAGE_JSON_PATH}').version")"
TAG_NAME="sdk-v${VERSION}"

cleanup() {
    rm -f "$NPMRC_PATH"
    if [[ -f "$PACKAGE_JSON_BACKUP" ]]; then
        mv "$PACKAGE_JSON_BACKUP" "$PACKAGE_JSON_PATH"
    fi
}

trap cleanup EXIT

if git -C "$ROOT_DIR" rev-parse --verify "$TAG_NAME" >/dev/null 2>&1; then
    echo "Tag ${TAG_NAME} already exists" >&2
    exit 1
fi

if npm view "${PACKAGE_NAME}@${VERSION}" version >/dev/null 2>&1; then
    echo "${PACKAGE_NAME}@${VERSION} is already published" >&2
    exit 1
fi

echo "Releasing ${PACKAGE_NAME} ${VERSION}"

pushd "$SDK_DIR" >/dev/null

cat > "$NPMRC_PATH" <<EOF
//registry.npmjs.org/:_authToken=${AUTH_TOKEN}
registry=https://registry.npmjs.org/
always-auth=true
EOF

export NPM_CONFIG_USERCONFIG="$NPMRC_PATH"

if [[ -n "$PACKAGE_OVERRIDE" ]]; then
    cp "$PACKAGE_JSON_PATH" "$PACKAGE_JSON_BACKUP"
    PACKAGE_JSON_PATH="$PACKAGE_JSON_PATH" PACKAGE_NAME="$PACKAGE_OVERRIDE" node <<'EOF'
const fs = require('fs');
const path = process.env.PACKAGE_JSON_PATH;
const pkg = JSON.parse(fs.readFileSync(path, 'utf8'));
pkg.name = process.env.PACKAGE_NAME;
fs.writeFileSync(path, `${JSON.stringify(pkg, null, 2)}\n`);
EOF
fi

echo "==> Installing dependencies"
bun install

echo "==> Building dist"
bun run build

echo "==> Typechecking"
bun run typecheck

echo "==> Validating npm package"
npm pack --dry-run

echo "==> Publishing to npm"
npm publish

popd >/dev/null

echo "==> Creating git tag ${TAG_NAME}"
git -C "$ROOT_DIR" tag "$TAG_NAME"
git -C "$ROOT_DIR" push origin "$TAG_NAME"

if [[ "$SKIP_GH_RELEASE" != true ]]; then
    echo "==> Creating GitHub release"
    gh release create "$TAG_NAME" \
        --repo "dojoengine/torii" \
        --title "${PACKAGE_NAME} v${VERSION}" \
        --generate-notes \
        --notes "Published ${PACKAGE_NAME} v${VERSION} to npm."
fi

echo "Release complete: ${PACKAGE_NAME} ${VERSION}"
