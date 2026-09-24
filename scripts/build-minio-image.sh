#!/bin/sh
set -eu

MINIO_RELEASE="${MINIO_RELEASE:-RELEASE.2025-10-15T17-29-55Z}"
MINIO_IMAGE="${MINIO_IMAGE:-local/minio:${MINIO_RELEASE}}"
BUILD_ROOT="${BUILD_ROOT:-${TMPDIR:-/tmp}}"
SOURCE_DIR="${BUILD_ROOT}/minio-source-${MINIO_RELEASE}"

if ! command -v git >/dev/null 2>&1; then
  echo "git is required to build MinIO." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to build MinIO." >&2
  exit 1
fi

if [ -e "${SOURCE_DIR}" ]; then
  echo "Source directory already exists: ${SOURCE_DIR}" >&2
  echo "Remove it or set BUILD_ROOT to another directory." >&2
  exit 1
fi

cleanup() {
  rm -rf "${SOURCE_DIR}"
}
trap cleanup EXIT INT TERM

echo "Cloning MinIO ${MINIO_RELEASE}..."
git clone --depth 1 --branch "${MINIO_RELEASE}" \
  https://github.com/minio/minio.git "${SOURCE_DIR}"

echo "Building ${MINIO_IMAGE}..."
(
  cd "${SOURCE_DIR}"
  TAG="${MINIO_IMAGE}" make docker
)

echo "Built ${MINIO_IMAGE}"
docker image inspect "${MINIO_IMAGE}" >/dev/null
