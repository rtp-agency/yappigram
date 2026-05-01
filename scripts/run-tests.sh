#!/usr/bin/env bash
# Run the yappigram regression test suite.
#
# Usage:
#     ./scripts/run-tests.sh                    # run everything
#     ./scripts/run-tests.sh -k pin             # by keyword
#     ./scripts/run-tests.sh tests/test_pin.py  # by path
#     ./scripts/run-tests.sh -v --tb=long       # extra pytest args
#
# Architecture:
#   - Test PG + Redis come from `docker-compose.test.yml` (ports 55433
#     and 56380, tmpfs storage, isolated from prod/dev compose stacks).
#   - pytest runs INSIDE a one-shot container built from
#     `backend/Dockerfile.test` (mirrors prod Python 3.11 + libpq +
#     asyncpg). Runs cross-platform; no local Python setup needed.
#   - Source is mounted as a volume so editing tests + rerunning is
#     instant (no image rebuild).
#
# Safety:
#   - Test container talks to `host.docker.internal:55433` /
#     `:56380` — matches the test-pg/redis we just brought up.
#   - conftest.py refuses to run if DATABASE_URL mentions any prod
#     hostname (defense in depth).
#   - First run rebuilds the test image; subsequent runs reuse it
#     (~1s startup).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Inside the test runner container, "127.0.0.1" means the container
# itself — NOT the host. On every platform we need `host.docker.internal`
# pointing at the host's loopback so it can reach the test PG/Redis on
# 55433/56380. Mac and Windows Docker Desktop wire that name in
# automatically; Linux needs `--add-host=host.docker.internal:host-gateway`.
TEST_DB_HOST="host.docker.internal"
TEST_NETWORK_ARGS="--add-host=host.docker.internal:host-gateway"

TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql+asyncpg://tgcrm_test:tgcrm_test@${TEST_DB_HOST}:55433/tgcrm_test}"
TEST_REDIS_URL="${TEST_REDIS_URL:-redis://${TEST_DB_HOST}:56380/0}"

echo "→ Starting test containers (pg:55433, redis:56380)…"
docker compose -f docker-compose.test.yml up -d --wait

cleanup() {
    echo "→ Stopping test containers…"
    docker compose -f docker-compose.test.yml down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "→ Building test runner image (cached after first run)…"
docker build -q -t yappigram-test-runner -f backend/Dockerfile.test backend/

echo "→ Running pytest…"
# Mount backend/ as /app so the test code (including conftest.py) is
# what's executed; runtime deps come from the image.
# MSYS_NO_PATHCONV=1 + MSYS2_ARG_CONV_EXCL stop Git Bash on Windows from
# rewriting the in-container `/app` path to a Windows path. No-op on
# Linux/Mac.
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run --rm -i \
    $TEST_NETWORK_ARGS \
    -e TEST_DATABASE_URL="$TEST_DATABASE_URL" \
    -e TEST_REDIS_URL="$TEST_REDIS_URL" \
    -e DATABASE_URL="$TEST_DATABASE_URL" \
    -e REDIS_URL="$TEST_REDIS_URL" \
    -v "$REPO_ROOT/backend:/app" \
    -w //app \
    yappigram-test-runner \
    pytest "$@"
