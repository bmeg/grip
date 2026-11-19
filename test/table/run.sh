#!/usr/bin/env bash
# Runs the conformance suite against a standalone filetable-backed SWAPI graph.
#
# usage: test/table/run.sh [conformance args...]
# env: GRIP=./grip RPC_PORT=18202 HTTP_PORT=18201
set -u

GRIP=${GRIP:-./grip}
RPC_PORT=${RPC_PORT:-18202}
HTTP_PORT=${HTTP_PORT:-18201}
WORK=$(mktemp -d)
PIDS=()

cleanup() {
	for pid in "${PIDS[@]}"; do
		kill "$pid" 2>/dev/null
	done
	wait 2>/dev/null
	rm -rf "$WORK"
}
trap cleanup EXIT

"$GRIP" server --rpc-port "$RPC_PORT" --http-port "$HTTP_PORT" \
	--config test/table.yml >"$WORK/server.log" 2>&1 &
PIDS+=($!)

for i in $(seq 1 60); do
	curl -sf "http://localhost:$HTTP_PORT/v1/graph" >/dev/null 2>&1 && break
	sleep 1
done

python conformance/run_conformance.py "http://localhost:$HTTP_PORT" \
	basic labels has keycheck fields null count distinct sort path render select totype struct mark group \
	--readOnly swapi "$@"
status=$?
if [ $status -ne 0 ]; then
	echo "--- server log (tail) ---"
	tail -n 40 "$WORK/server.log"
fi
exit $status
