# Postgres Plugin Prototype (Rust + pgrx)

This prototype shows what an independent PostgreSQL extension could look like for delegated GripQL traversal execution.

## Goal

Expose one stable SQL entrypoint that GRIP can call:

- schema: `grip_ext`
- function: `grip_exec(graph text, query jsonb)`
- return: `SETOF jsonb`

For smoke testing from `psql`, the extension also exposes:

- `grip_ext.grip_ping() -> text`

The input `query` is a JSON serialization of `gripql.QuerySet`.
Each output row is one GRIP query result item encoded as JSON.

## Result Row Shape (prototype)

Every row is a JSON object with a `result_type` discriminator:

- count row:
  - `{ "result_type": "count", "count": 42 }`
- vertex row:
  - `{ "result_type": "vertex", "vertex": { "id": "v1", "label": "Person", "data": { ... } } }`
- edge row:
  - `{ "result_type": "edge", "edge": { "id": "e1", "label": "knows", "from": "v1", "to": "v2", "data": { ... } } }`
- render row:
  - `{ "result_type": "render", "render": { ... } }`

The prototype ships with a tiny in-memory sample graph so these queries work
without any external tables:

- `v1` = `Person` with `name = Alice`
- `v2` = `Person` with `name = Bob`
- `v3` = `City` with `name = Paris`
- `e1` = `v1 -[knows]-> v2`
- `e2` = `v1 -[lives_in]-> v3`

## Minimal Development Flow

1. Install pgrx tooling and PostgreSQL dev headers.
2. Build/install extension from `rust/`.
3. Apply `sql/grip_ext--0.1.0.sql`.
4. Call:

```sql
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

## Notes

- This prototype intentionally focuses on read-only traversals.
- Statement subset validation should match the guard in `psqlx/compiler.go`.
- GRIP should treat extension errors as deterministic user-visible query errors.
- If you are testing manually, start with `grip_ping()`, then `v`, `has_label`,
  `count`, and `out` queries before attempting anything more complex.
