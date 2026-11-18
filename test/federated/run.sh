#!/usr/bin/env bash
# Runs the conformance suite against a federated SWAPI graph: one GRIPPER resource
# node per ID prefix (so every edge between labels crosses nodes) behind one central
# GRIP server.
#
# usage: test/federated/run.sh [conformance args...]
# env:   GRIP=./grip  RPC_PORT=18202  HTTP_PORT=18201  NODE_BASE_PORT=50061
set -u

GRIP=${GRIP:-./grip}
RPC_PORT=${RPC_PORT:-18202}
HTTP_PORT=${HTTP_PORT:-18201}
NODE_BASE_PORT=${NODE_BASE_PORT:-50061}
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

PREFIXES=$(python test/federated/split.py conformance/graphs/swapi.vertices conformance/graphs/swapi.edges "$WORK/data") || exit 1

CONF="$WORK/server.yml"
{
	echo "Default: swapi"
	echo "Drivers:"
	echo "  swapi:"
	echo "    Gripper:"
	echo "      Graph: swapi"
	echo "Server:"
	echo "  HTTPPort: \"$HTTP_PORT\""
	echo "  RPCPort: \"$RPC_PORT\""
	echo "  WorkDir: $WORK/work"
	echo "Sources:"
} >"$CONF"

port=$NODE_BASE_PORT
for prefix in $PREFIXES; do
	label=${prefix%:}
	"$GRIP" node --prefix "$prefix" --name "$label" --listen "localhost:$port" \
		--driver pebble --db "$WORK/$label.db" \
		--vertices "$WORK/data/$label.vertices" --edges "$WORK/data/$label.edges" \
		>"$WORK/node-$label.log" 2>&1 &
	PIDS+=($!)
	echo "  $label: localhost:$port" >>"$CONF"
	port=$((port + 1))
done

# wait for the nodes to load their data and listen
for i in $(seq 1 60); do
	ready=0
	p=$NODE_BASE_PORT
	for _ in $PREFIXES; do
		(exec 3<>"/dev/tcp/localhost/$p") 2>/dev/null && ready=$((ready + 1))
		p=$((p + 1))
	done
	[ "$ready" -eq "$(echo "$PREFIXES" | wc -w)" ] && break
	sleep 1
done

"$GRIP" server --config "$CONF" >"$WORK/server.log" 2>&1 &
PIDS+=($!)
for i in $(seq 1 60); do
	curl -sf "http://localhost:$HTTP_PORT/v1/graph" >/dev/null 2>&1 && break
	sleep 1
done

python conformance/run_conformance.py "http://localhost:$HTTP_PORT" --readOnly swapi "$@"
status=$?
if [ $status -ne 0 ]; then
	echo "--- server log (tail) ---"
	tail -n 40 "$WORK/server.log"
fi
exit $status
