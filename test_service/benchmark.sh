#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${ROOT_DIR}/test_service/start.sh"
trap 'cd "${ROOT_DIR}" && ./test_service/stop.sh' EXIT

cd "${ROOT_DIR}"
uv run --no-cache python -m benchmarks.nsq_benchmark "$@"
