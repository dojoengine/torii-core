#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SDK_DIR="${ROOT_DIR}/torii.js"
SKIP_GH_RELEASE=false

for arg in "$@"; do
    case "$arg" in
        --skip-gh-release)
            SKIP_GH_RELEASE=true
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [--skip-gh-release]" >&2
            exit 1
            ;;
    esac
done

if ! command -v bun >/dev/null 2>&1; then
    echo "bun is required" >&2
    exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required" >&2
    exit 1
fi

if ! command -v git >/dev/null 2>&1; then
    echo "git is required" >&2
    exit 1
fi

if ! command -v node >/dev/null 2>&1; then
    echo "node is required" >&2
    exit 1
fi

if [[ -z "${NODE_AUTH_TOKEN:-}" ]]; then
    echo "NODE_AUTH_TOKEN is required" >&2
    exit 1
fi

if [[ "$SKIP_GH_RELEASE" != true ]] && ! command -v gh >/dev/null 2>&1; then
    echo "gh is required unless --skip-gh-release is used" >&2
    exit 1
fi

if ! git -C "$ROOT_DIR" diff --quiet || ! git -C "$ROOT_DIR" diff --cached --quiet; then
    echo "Git working tree must be clean before publishing" >&2
    exit 1
fi

PACKAGE_NAME="$(node -p "require('${SDK_DIR}/package.json').name")"
VERSION="$(node -p "require('${SDK_DIR}/package.json').version")"
TAG_NAME="sdk-v${VERSION}"

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
