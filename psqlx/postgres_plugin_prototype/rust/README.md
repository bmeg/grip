# Rust Extension Quickstart

This directory is the runnable proof-of-concept for the Postgres extension.

## Prerequisites

- Rust toolchain (`rustup` recommended)
- `cargo-pgrx`
- PostgreSQL development headers and `pg_config`

## One-time setup

```bash
cargo install --locked cargo-pgrx
cargo pgrx init --pg15 /path/to/pg_config
```

If your local Postgres is already installed and writable, point `--pg15` at its
`pg_config`. Otherwise, let `cargo pgrx init` download and manage a local dev
cluster.

## Build and install

```bash
make install
```

or explicitly:

```bash
cargo pgrx install pg15
```

## Open psql

```bash
make run
```

This starts the pgrx-managed Postgres instance and drops you into `psql`.

## Manual smoke tests

Inside `psql`:

```sql
CREATE EXTENSION grip_ext;

SELECT grip_ext.grip_ping();

SELECT * FROM grip_ext.grip_exec(
  'test-graph',
  '{"query":[{"v":[]}]} '::jsonb
);

SELECT * FROM grip_ext.grip_exec(
  'test-graph',
  '{"query":[{"v":[]},{"has_label":["Person"]},{"count":{}}]}'::jsonb
);

SELECT * FROM grip_ext.grip_exec(
  'test-graph',
  '{"query":[{"v":["v1"]},{"out":["knows"]}]}'::jsonb
);
```

## Expected behavior

- `grip_ping()` returns `grip_ext ready`
- `count` queries return a single JSON row with `result_type = count`
- `out` from `v1` on label `knows` returns `v2`
