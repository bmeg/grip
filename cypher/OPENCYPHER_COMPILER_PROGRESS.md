# OpenCypher Compiler Progress and Remaining TODOs

Date: 2026-03-16

This document summarizes the current implementation status of the read-only OpenCypher to GripQL compiler, plus prioritized follow-up work.

## Scope Goal

Build a minimal read-only OpenCypher translator that compiles a constrained subset of Cypher into GripQL query builder steps.

## Implemented So Far

### 1. Read-only guardrails and explicit unsupported errors

Implemented in `cypher/compiler/build.go`.

Behavior:

- Unsupported clauses fail fast with explicit errors.
- No silent fallback to partial queries for unsupported grammar.

Currently rejected clause families:

- `CREATE`
- `WITH`
- `SET`
- `DELETE`
- `REMOVE`
- `MERGE`
- `UNWIND`
- `UNION`
- multipart query forms
- variable-length relationships (`*1..2`)

### 2. Node and relationship pattern translation

Implemented in `cypher/compiler/build.go`.

Supported:

- Node labels and inline map properties from `MATCH` patterns
- Linear relationship traversal chains
- Relationship direction mapping
  - `-[:TYPE]->` => `Out("TYPE")`
  - `<-[:TYPE]-` => `In("TYPE")`
  - `-[:TYPE]-` => `Both("TYPE")`

### 3. WHERE support (minimal)

Implemented in `cypher/compiler/build.go`.

Supported predicate shape:

- `var.field <op> literal`

Supported operators:

- `=`
- `!=`
- `<>`
- `>`
- `>=`
- `<`
- `<=`

Supported literals:

- quoted strings
- integers
- floats
- booleans
- `null`

Current WHERE scope rule:

- `var` must be the current traversal variable (last bound node in current path).

### 4. RETURN projection support

Implemented in `cypher/compiler/build.go`.

Supported:

- `RETURN var`
- `RETURN var.field`
- `RETURN var.field AS alias`
- multi-item combinations of the above (render map)

### 5. ORDER BY support (minimal)

Implemented in `cypher/compiler/build.go`.

Supported:

- `ORDER BY var.field`
- `ORDER BY var.field ASC`
- `ORDER BY var.field DESC`
- multiple comma-separated sort keys

Current ORDER BY scope rule:

- each `var` must match the current traversal variable.

### 6. SKIP and LIMIT support

Implemented in `cypher/compiler/build.go`.

Supported:

- `SKIP <int-literal>`
- `LIMIT <int-literal>`

Notes:

- Expressions in `SKIP`/`LIMIT` are not yet supported.
- Query assembly currently emits projection before pagination (`Render(...).Skip(...).Limit(...)`).

### 7. Developer documentation updates

Added and updated:

- `cypher/CYPHER_TO_GRIPQL.md`
- `cypher/README.md` (link)
- root `README.md` (link)

### 8. Tests

Primary test file:

- `cypher/test/cypher_test.go`

Coverage currently includes:

- simple node MATCH
- directional and undirected relationship traversals
- multi-hop linear traversal case
- simple WHERE predicates
- RETURN property and alias projections
- ORDER BY ASC/DESC and multi-key sorting
- SKIP/LIMIT numeric clauses
- negative cases for unsupported constructs

## Known Constraints / Gaps

1. WHERE is intentionally narrow

- No `AND` / `OR` composition
- No parentheses or nested boolean expressions
- No function calls in predicates
- No cross-variable filtering except current-variable-only rule

2. RETURN expression support is intentionally narrow

- No function projections (for example `count(n)`)
- No arithmetic expressions
- No nested object/list expression rendering

3. ORDER BY expression support is intentionally narrow

- Only `var.field` form
- No ordering by function/expression
- No ordering by non-current traversal variable

4. Aggregations and pipelines are not implemented

- `WITH ... count(...)` and similar pipelines are unsupported
- No explicit aggregate translation layer yet

5. Duplicate compiler path still exists

- `endpoints/cypher/translate/build.go` remains a separate implementation and is not yet delegated to `cypher/compiler` as single source of truth.

## TODOs (Prioritized)

### P0: Correctness and architecture

1. Unify compiler entrypoints

- Make endpoint translator call into `cypher/compiler` instead of maintaining duplicate logic.
- Ensure one source of truth for parse + translation behavior.

2. Add explicit compile error typing

- Replace plain string errors with structured error categories (unsupported-clause, invalid-shape, unsupported-expression, etc.).

3. Harden path and variable validation

- Validate duplicate variable bindings and ambiguous variable reuse patterns.

### P1: Query feature expansion

1. WHERE boolean composition

- Add support for `AND`/`OR` over simple comparison predicates.
- Preserve strict variable-scope checks initially.

2. RETURN refinements

- Decide and implement explicit behavior for returning full node (`$var`) vs data-only (`$var._data`) consistency.
- Add optional support for scalar-only output shape in single-projection mode if desired.

3. ORDER BY refinements

- Optionally allow ordering by projected alias where deterministic.
- Decide support policy for ordering on previously bound variables.

### P2: Extended Cypher support

1. Optional support for simple aggregation patterns

- Minimal `WITH n, count(x) AS c` pipeline support.

2. Optional support for variable-length relationship ranges

- Controlled subset (for example fixed small ranges) or explicit planner restrictions.

3. Optional support for DISTINCT

- Map straightforward `RETURN DISTINCT` forms to GripQL distinct semantics.

### P3: Testing and docs hardening

1. Expand table-driven cases in `cypher/test/cypher_test.go`

- Add more edge cases for whitespace/casing variations and mixed clause ordering.

2. Add a dedicated negative-tests section

- Ensure unsupported grammar shapes always produce stable explicit errors.

3. Keep docs synchronized

- Update `cypher/CYPHER_TO_GRIPQL.md` for each newly supported feature.
- Add examples only after tests pass.

## Suggested Next Implementation Step

Implement WHERE boolean composition (`AND` and `OR`) over the existing simple predicate parser, then expand tests accordingly.

This gives the highest user-visible gain while staying inside read-only scope and existing architecture.
