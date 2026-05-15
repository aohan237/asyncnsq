#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_MAX_PROCS="${GO_MAX_PROCS:-}"

"${ROOT_DIR}/test_service/start.sh"
trap 'cd "${ROOT_DIR}" && ./test_service/stop.sh' EXIT

cd "${ROOT_DIR}/benchmarks/go_nsq_baseline"
if [[ -n "${GO_MAX_PROCS}" ]]; then
  GOMAXPROCS="${GO_MAX_PROCS}" go run . "$@"
else
  go run . "$@"
fi
