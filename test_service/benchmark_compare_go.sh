#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${PROFILE:-quick}"
SCENARIOS="${SCENARIOS:-pub,mpub,e2e}"
OUTPUT_DIR="${OUTPUT_DIR:-benchmark-results}"
GO_MAX_PROCS="${GO_MAX_PROCS:-}"

"${ROOT_DIR}/test_service/start.sh"
trap 'cd "${ROOT_DIR}" && ./test_service/stop.sh' EXIT

mkdir -p "${ROOT_DIR}/${OUTPUT_DIR}"

cd "${ROOT_DIR}"
uv run --no-cache python -m benchmarks.nsq_benchmark \
  --profile "${PROFILE}" \
  --scenarios "${SCENARIOS}" \
  --markdown "${OUTPUT_DIR}/asyncnsq-benchmark.md" \
  --json "${OUTPUT_DIR}/asyncnsq-benchmark.json" \
  "$@"

cd "${ROOT_DIR}/benchmarks/go_nsq_baseline"
if [[ -n "${GO_MAX_PROCS}" ]]; then
  GOMAXPROCS="${GO_MAX_PROCS}" go run . \
    --profile "${PROFILE}" \
    --scenarios "${SCENARIOS}" \
    --markdown "${ROOT_DIR}/${OUTPUT_DIR}/go-nsq-benchmark.md" \
    --json "${ROOT_DIR}/${OUTPUT_DIR}/go-nsq-benchmark.json" \
    "$@"
else
  go run . \
    --profile "${PROFILE}" \
    --scenarios "${SCENARIOS}" \
    --markdown "${ROOT_DIR}/${OUTPUT_DIR}/go-nsq-benchmark.md" \
    --json "${ROOT_DIR}/${OUTPUT_DIR}/go-nsq-benchmark.json" \
    "$@"
fi

cat <<EOF

Benchmark comparison reports:
  ${OUTPUT_DIR}/asyncnsq-benchmark.md
  ${OUTPUT_DIR}/go-nsq-benchmark.md
EOF
