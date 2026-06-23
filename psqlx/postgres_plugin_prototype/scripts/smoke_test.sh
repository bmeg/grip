#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd "$script_dir/.." && pwd)

cd "$root_dir"

for _ in $(seq 1 30); do
  if docker compose exec -T postgres pg_isready -U postgres -d grip_plugin_dev >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d grip_plugin_dev <<'SQL'
SELECT extname FROM pg_extension WHERE extname = 'grip_ext';

SELECT grip_ext.grip_ping();

SELECT *
FROM grip_ext.grip_exec(
  'test-graph',
  '{"query":[{"v":[]},{"has_label":["Person"]},{"count":{}}]}'::jsonb
);

SELECT *
FROM grip_ext.grip_exec(
  'test-graph',
  '{"query":[{"v":["v1"]},{"out":["knows"]}]}'::jsonb
);
SQL