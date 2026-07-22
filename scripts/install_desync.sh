#!/usr/bin/env bash
set -euo pipefail

DESYNC_VERSION=${DESYNC_VERSION:-1.0.3}
INSTALL_DIR=${1:-"${HOME}/.local/bin"}
OS_NAME=$(uname -s | tr '[:upper:]' '[:lower:]')
MACHINE=$(uname -m)

case "${MACHINE}" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *)
    echo "unsupported architecture: ${MACHINE}" >&2
    exit 2
    ;;
esac

case "${OS_NAME}" in
  linux|darwin) ;;
  *)
    echo "unsupported operating system: ${OS_NAME}" >&2
    exit 2
    ;;
esac

ARCHIVE="desync_${DESYNC_VERSION}_${OS_NAME}_${ARCH}.tar.gz"
BASE_URL="https://github.com/folbricht/desync/releases/download/v${DESYNC_VERSION}"
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/stockagent-desync.XXXXXX")
trap 'rm -rf -- "${WORK_DIR}"' EXIT

curl --fail --location --silent --show-error \
  --output "${WORK_DIR}/${ARCHIVE}" "${BASE_URL}/${ARCHIVE}"
curl --fail --location --silent --show-error \
  --output "${WORK_DIR}/checksums.txt" "${BASE_URL}/checksums.txt"
(
  cd "${WORK_DIR}"
  grep "  ${ARCHIVE}$" checksums.txt | sha256sum --check --strict -
  tar -xzf "${ARCHIVE}"
)

mkdir -p -- "${INSTALL_DIR}"
install -m 0755 "${WORK_DIR}/desync" "${INSTALL_DIR}/desync"
echo "installed desync v${DESYNC_VERSION}: ${INSTALL_DIR}/desync"
