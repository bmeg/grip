#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd "$script_dir/.." && pwd)
package_dir_input=${1:-$root_dir/build/pg15-package}
if [[ "$package_dir_input" = /* ]]; then
  package_dir="$package_dir_input"
else
  package_dir="$root_dir/$package_dir_input"
fi
pg_config=/usr/lib/postgresql/15/bin/pg_config

export PGRX_HOME=${PGRX_HOME:-$root_dir/.pgrx}
export PATH=/root/.cargo/bin:$PATH

mkdir -p "$PGRX_HOME"

if [[ ! -f "$PGRX_HOME/config.toml" ]]; then
  cargo pgrx init --pg15 "$pg_config"
fi

rm -rf "$package_dir"
mkdir -p "$package_dir"

cd "$root_dir/rust"
cargo pgrx package \
  --manifest-path Cargo.toml \
  --pg-config "$pg_config" \
  --out-dir "$package_dir"