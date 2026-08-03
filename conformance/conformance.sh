#!/bin/bash
# ============================================================================
# conformance.sh — All-in-one GRIP conformance test runner
#
# Usage:
#   ./conformance.sh <backend>                        # run tests, print results
#   ./conformance.sh <backend> --output results.txt   # write results to file
#   ./conformance.sh <backend> basic                  # run only 'basic' module
#   ./conformance.sh <backend> --list                  # list available backends
#   ./conformance.sh -h                                # show this help
#
# Supported backends: arango, mongo, postgres, badger, pebble, sqlite
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HTTP_PORT=18201
RPC_PORT=18202
SERVER_PID=""

# ── colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        info "Stopping GRIP server (PID $SERVER_PID) ..."
        kill "$SERVER_PID" 2>/dev/null || true
        for _ in $(seq 1 10); do
            kill -0 "$SERVER_PID" 2>/dev/null && sleep 1 || break
        done
        kill -0 "$SERVER_PID" 2>/dev/null && kill -9 "$SERVER_PID" 2>/dev/null || true
        info "GRIP server stopped."
    fi
}
trap cleanup EXIT INT TERM

wait_for_port() {
    local port="$1" label="$2" max_wait="${3:-30}"
    for i in $(seq 1 "$max_wait"); do
        if port_is_listening 127.0.0.1 "$port"; then
            info "${label} is ready on port ${port}."
            return 0
        fi
        sleep 1
    done
    error "${label} did not become ready within ${max_wait}s on port ${port}."
    return 1
}

port_is_listening() {
    local host="$1" port="$2"
    python - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

try:
    with socket.create_connection((host, port), timeout=1):
        pass
except OSError:
    sys.exit(1)
PY
}

# ── build GRIP ────────────────────────────────────────────────────────────────
build_grip() {
    info "Building GRIP binary ..."
    cd "$REPO_ROOT"
    go build ./
    info "GRIP binary ready."
}

# ── backend configs ───────────────────────────────────────────────────────────
get_config() {
    case "$1" in
        arango) echo "${REPO_ROOT}/test/arango.yml" ;;
        mongo) echo "${REPO_ROOT}/test/mongo.yml" ;;
        postgres) echo "${REPO_ROOT}/test/psql.yml" ;;
        badger) echo "${REPO_ROOT}/test/badger.yml" ;;
        pebble) echo "${REPO_ROOT}/test/pebble.yml" ;;
        *) echo "" ;;
    esac
}

# ── start backends ────────────────────────────────────────────────────────────
start_backend() {
    local backend="$1"
    case "$backend" in
        arango)
            info "Starting ArangoDB container ..."
            docker rm -f grip-arango-test >/dev/null 2>&1 || true
            docker run -d --name grip-arango-test \
                -p 8529:8529 \
                -e ARANGO_ROOT_PASSWORD=openSesame \
                arangodb:3.12 >/dev/null
            info "Waiting for ArangoDB ..."
            for i in $(seq 1 60); do
                if curl -sf -u root:openSesame http://localhost:8529/_api/version >/dev/null 2>&1; then
                    info "ArangoDB is ready."
                    return 0
                fi
                sleep 2
            done
            error "ArangoDB did not start within 60s."
            docker logs grip-arango-test >&2 || true
            return 1
            ;;

        mongo)
            info "Starting MongoDB container ..."
            docker rm -f grip-mongodb-test >/dev/null 2>&1 || true
            docker run -d --name grip-mongodb-test \
                -p 27017:27017 \
                mongo:7.0.13-rc0-jammy >/dev/null
            wait_for_port 27017 "MongoDB" 30
            ;;

        postgres)
            info "Starting PostgreSQL container ..."
            docker rm -f grip-postgres-test >/dev/null 2>&1 || true
            docker run -d --name grip-postgres-test \
                -p 15432:5432 \
                -e POSTGRES_PASSWORD= \
                -e POSTGRES_USER=postgres \
                postgres:10.4 >/dev/null
            wait_for_port 15432 "PostgreSQL" 30
            ;;

        badger)
            info "Badger uses embedded storage — no docker backend needed."
            return 0
            ;;

        pebble)
            info "Pebble uses embedded storage — no docker backend needed."
            return 0
            ;;

        *)
            error "Unknown backend: $backend"
            exit 1
            ;;
    esac
}

# ── start GRIP server ─────────────────────────────────────────────────────────
start_server() {
    local config="$1"
    info "Starting GRIP server with config: ${config}"
    cd "$REPO_ROOT"
    ./grip server --rpc-port "$RPC_PORT" --http-port "$HTTP_PORT" --config "$config" --verbose \
        &>/tmp/grip-server.log &
    SERVER_PID=$!

    for i in $(seq 1 15); do
        if curl -sf http://localhost:${HTTP_PORT}/ >/dev/null 2>&1 || \
           port_is_listening 127.0.0.1 "$HTTP_PORT"; then
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

# ── run conformance ───────────────────────────────────────────────────────────
run_conformance() {
    local backend="$1"
    shift
    local output_file=""
    local console_output_file=""
    local extra_args=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --output)
                output_file="$2"
                shift 2
                ;;
            *)        extra_args+=("$1");   shift ;;
        esac
    done

    # Build command
    local cmd="python ${SCRIPT_DIR}/run_conformance.py http://localhost:${HTTP_PORT}"

    # Backend-specific excludes
    case "$backend" in
        mongo) extra_args+=(--exclude nested_index) ;;
    esac

    if [[ -n "$output_file" ]]; then
        extra_args+=(--output "$output_file")
    fi

    if [[ ${#extra_args[@]} -gt 0 ]]; then
        cmd+=" $(printf '%q ' "${extra_args[@]}")"
    fi

    info "Running: $cmd"

    local rc=0
    if [[ -n "$output_file" ]]; then
        console_output_file="${output_file}.log"
        eval "$cmd" > "$console_output_file" 2>&1 || rc=$?
        if [[ -f /tmp/grip-server.log ]]; then
            cp /tmp/grip-server.log "${output_file}.server_log"
            info "Server log copied to ${output_file}.server_log"
        fi
        info "Console log written to ${console_output_file}; YAML results written to ${output_file}."
    else
        eval "$cmd" || rc=$?
    fi
    return $rc
}

# ── list backends ─────────────────────────────────────────────────────────────
list_backends() {
    echo "Available backends:"
    for b in arango mongo postgres badger pebble sqlite; do
        echo "  ${b}"
    done
}

# ── main ──────────────────────────────────────────────────────────────────────
main() {
    if [[ $# -eq 0 ]] || [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
        sed -n '2,14p' "$0"
        exit 0
    fi

    local backend="$1"
    shift

    case "$backend" in
        --list|-l) list_backends; exit 0 ;;
    esac

    # Validate backend
    local found=0
    for b in arango mongo postgres badger pebble sqlite; do
        [[ "$b" == "$backend" ]] && found=1
    done
    if [[ $found -eq 0 ]]; then
        error "Unknown backend '$backend'. Supported: arango mongo postgres badger pebble sqlite"
        list_backends
        exit 1
    fi

    cd "$REPO_ROOT"

    # Build GRIP (idempotent — only rebuilds when sources are newer)
    build_grip

    # Start docker backend if needed
    start_backend "$backend"

    # Get config file
    local config
    config="$(get_config "$backend")"
    if [[ -z "$config" || ! -f "$config" ]]; then
        error "No valid config file for backend: $backend"
        exit 1
    fi

    # Start GRIP server
    start_server "$config"

    # Run conformance tests (capture exit code)
    set +e
    run_conformance "$backend" "$@"
    local rc=$?
    set -e

    if [[ $rc -eq 0 ]]; then
        info "All conformance tests passed!"
    else
        error "Some conformance tests failed (exit code $rc)."
    fi

    return $rc
}

main "$@"

