#!/usr/bin/env bash
#
# Boot the Docker stack, run the migration against it, and diff row counts.
#
#   ./docker/test/run_test.sh            # boot, migrate, verify, then tear down
#   KEEP_UP=1 ./docker/test/run_test.sh  # leave containers running afterwards
#
# Exit code 0 = all tables match, non-zero = failure.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo ">> Starting containers..."
docker compose up -d

wait_healthy() {
  local name="$1" tries="${2:-150}" status
  echo ">> Waiting for $name to be healthy (Oracle's first run can take several minutes)..."
  for ((i = 1; i <= tries; i++)); do
    status="$(docker inspect --format '{{.State.Health.Status}}' "$name" 2>/dev/null || echo missing)"
    if [[ "$status" == "healthy" ]]; then
      echo "   $name: healthy"
      return 0
    fi
    sleep 5
  done
  echo "ERROR: $name did not become healthy in time." >&2
  docker compose logs --tail 50 "$name" >&2 || true
  return 1
}

wait_healthy ora2pg-oracle
wait_healthy ora2pg-postgres

echo ">> Creating Python venv and installing drivers..."
VENV="$ROOT/.venv-test"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet oracledb psycopg2-binary

echo ">> Running migration + row-count verification..."
set +e
"$VENV/bin/python" "$ROOT/docker/test/run_migration_test.py"
rc=$?
set -e

if [[ "${KEEP_UP:-0}" != "1" ]]; then
  echo ">> Tearing down containers (set KEEP_UP=1 to keep them running)..."
  docker compose down
else
  echo ">> Leaving containers running (KEEP_UP=1)."
fi

if [[ "$rc" -eq 0 ]]; then
  echo ">> TEST PASSED"
else
  echo ">> TEST FAILED (exit $rc)"
fi
exit "$rc"
