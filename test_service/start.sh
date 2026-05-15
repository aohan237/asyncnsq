#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

docker compose -f "${COMPOSE_FILE}" up -d

for url in \
  "http://127.0.0.1:4161/ping" \
  "http://127.0.0.1:4151/ping" \
  "http://127.0.0.1:4251/ping" \
  "http://127.0.0.1:4351/ping"
do
  printf 'waiting for %s' "${url}"
  ok=0
  for _ in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      printf ' ok\n'
      ok=1
      break
    fi
    printf '.'
    sleep 1
  done
  if [ "${ok}" -ne 1 ]; then
    printf ' failed\n' >&2
    exit 1
  fi
done

cat <<'EOF'

NSQ test cluster is running:
  lookupd:  http://127.0.0.1:4161
  nsqd1:    tcp://127.0.0.1:4150  http://127.0.0.1:4151
  nsqd2:    tcp://127.0.0.1:4250  http://127.0.0.1:4251
  nsqd3:    tcp://127.0.0.1:4350  http://127.0.0.1:4351
  nsqadmin: http://127.0.0.1:4171

Run tests with:
  uv run python -m pytest
EOF
