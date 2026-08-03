#!/usr/bin/env bash
# =============================================================================
# run_benchmarks.sh – Execute GRIP benchmarks for a given backend.
#
# Usage:
#   ./run_benchmarks.sh <backend> [--output results.txt]
#   ./run_benchmarks.sh all          # run all supported backends sequentially
#
# Supported backends: arango, mongo, postgres, badger, pebble
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HTTP_PORT=18201
RPC_PORT=18202

# ---------------------------------------------------------------------------
# Logging helpers
info()  { echo -e "\033[0;32m[INFO]\033[0m $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m $*" >&2; }
error() { echo -e "\033[0;31m[ERROR]\033[0m $*" >&2; }

SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" && -d /proc/$SERVER_PID ]]; then
        info "Stopping GRIP server (PID $SERVER_PID) ..."
        kill "$SERVER_PID" 2>/dev/null || true
        for _ in $(seq 1 10); do
            kill -0 "$SERVER_PID" 2>/dev/null && sleep 1 || break
        done
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi

    # Remove temporary containers if any exist
    for name in grip-arango-test grip-mongodb-test grip-postgres-test; do
        docker rm -f "$name" &>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
wait_for_port() {
    local host="$1" port="$2" label="$3" max_wait="${4:-30}"
    for i in $(seq 1 "$max_wait"); do
        if nc -z "$host" "$port"; then
            info "${label} is ready on port ${port}."
            return 0
        fi
        sleep 1
    done
    error "${label} did not become ready within ${max_wait}s on port ${port}."
    exit 1
}

# ---------------------------------------------------------------------------
build_grip() {
    info "Building GRIP binary ..."
    cd "$REPO_ROOT"
    go build ./
    info "GRIP binary ready."
}

get_config() {
    case "$1" in
        arango) echo "$REPO_ROOT/test/arango.yml" ;;
        mongo)  echo "$REPO_ROOT/test/mongo.yml" ;;
        postgres) echo "$REPO_ROOT/test/psql.yml" ;;
        badger) echo "$REPO_ROOT/test/badger.yml" ;;
        pebble) echo "$REPO_ROOT/test/pebble.yml" ;;
        sqlite) echo "$REPO_ROOT/test/sqlite.yml" ;;
        grids) echo "$REPO_ROOT/test/grids.yml" ;;
        *)      echo "" ;;
    esac
}

start_backend() {
    local backend="$1"
    case "$backend" in
        arango)
            info "Starting ArangoDB container ..."
            docker rm -f grip-arango-test &>/dev/null || true
            docker run -d --name grip-arango-test \
                -p 8529:8529 \
                -e ARANGO_ROOT_PASSWORD=openSesame \
                arangodb:3.12 > /dev/null
            for i in $(seq 1 60); do
                if curl -sf -u root:openSesame http://localhost:8529/_api/version &>/dev/null; then
                    info "ArangoDB is ready."
                    return 0
                fi
                sleep 2
            done
            error "ArangoDB did not start within 60s."
            docker logs grip-arango-test >&2 || true
            exit 1
            ;;
        mongo)
            info "Starting MongoDB container ..."
            docker rm -f grip-mongodb-test &>/dev/null || true
            docker run -d --name grip-mongodb-test \
                -p 27017:27017 \
                mongo:7.0.13-rc0-jammy > /dev/null
            wait_for_port 127.0.0.1 27017 "MongoDB"
            ;;
        postgres)
            info "Starting PostgreSQL container ..."
            docker rm -f grip-postgres-test &>/dev/null || true
            docker run -d --name grip-postgres-test \
                -p 15432:5432 \
                -e POSTGRES_PASSWORD= \
                -e POSTGRES_USER=postgres \
                postgres:10.4 > /dev/null
            wait_for_port 127.0.0.1 15432 "PostgreSQL"
            ;;
        badger|pebble|sqlite|grids)
            info "$backend uses embedded storage - no docker needed."
            return 0
            ;;
        *)
            error "Unknown backend: $backend"
            exit 1
            ;;
    esac
}

start_server() {
    local config="$1"
    info "Starting GRIP server with config: ${config:-<embedded>}"
    cd "$REPO_ROOT"
    ./grip server --rpc-port "$RPC_PORT" --http-port "$HTTP_PORT" \
        $( [ -n "$config" ] && echo "--config $config" ) \
        --verbose &>/tmp/grip-server.log &
    SERVER_PID=$!
    for i in $(seq 1 15); do
        if curl -sf http://localhost:${HTTP_PORT}/ >/dev/null 2>&1 || \
           nc -z localhost $HTTP_PORT; then
            info "GRIP server is ready (PID $SERVER_PID)."
            return 0
        fi
        sleep 1
    done
    error "GRIP server did not start within 15s."
    head -30 /tmp/grip-server.log >&2
    kill "$SERVER_PID" 2>/dev/null || true
    exit 1
}

run_benchmark() {
    local backend="$1"
    local out_file="bench-${backend}.json"
    info "Running benchmark for $backend - output to ${out_file}"
    go run "$REPO_ROOT/benchmark/graphbench-cli/main.go" \
        --server "localhost:${RPC_PORT}" --output "$out_file" --backend "$backend"
}

# ---------------------------------------------------------------------------
main() {
    if [[ $# -lt 1 ]]; then
        echo "Usage: $0 <backend|all> [--output file]"
        exit 1
    fi
    local backend="$1"
    shift

    build_grip

    backends=()
    if [[ "$backend" == "all" ]]; then
        backends=(arango mongo postgres badger pebble sqlite)
    else
        backends=($backend)
    fi

    for b in "${backends[@]}"; do
        info "=== Benchmarking backend: $b ==="
        start_backend "$b"
        config=$(get_config "$b")
        start_server "$config"
        run_benchmark "$b"
        cleanup  # stop server and containers for this round
    done
}

main "$@"
