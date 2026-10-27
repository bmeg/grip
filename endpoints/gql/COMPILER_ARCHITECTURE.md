# GQL Compiler Architecture and Extension Guide

Companion to `GQL_TO_GRIPQL.md` (what maps to what) and `GQL_COMPILER_PROGRESS.md` (status and TODOs).
This document explains *how* the compiler is organized and *where* to change code when extending it.

## Package Layout

| Path | Purpose |
|---|---|
| `compiler/build.go` | The entire translator: ANTLR listener + clause compilers + query assembly. Single source of truth. |
| `parser/` | Generated ANTLR Go code for the ISO GQL grammar (`opengql/grammar`). **Do not edit by hand**; regenerate with `./generate.sh`. |
| `handle.go` | HTTP endpoint: `POST /gql/{graphName}` with `{"query": "..."}`. Compiles via `compiler.RunParser`, executes via `gripql.Client.Traversal`. |
| `test/gql_test.go` | Table-driven compiler tests: GQL text → expected `*gripql.Query` (compared with `proto.Equal` per statement), or expected compile error. |
| `cmd/gql/main.go` | CLI client: `grip gql <graph> <GQL query>`. |

## Compilation Flow

```
GQL text
  → GQLLexer + syntaxErrorListener        (collects syntax errors with line:col)
  → GQLParser.GqlProgram()                (builds parse tree)
  → ParseTreeWalkerDefault.Walk(listener) (gqlListener records clause state)
  → BuildQuery()                          (validates + assembles *gripql.Query)
```

Entry point is `RunParser(gqlText string) (*gripql.Query, error)`. Any failure — syntax error,
unsupported feature, or invalid query — returns an error; partial queries are never emitted.

### Phase 1: listener state collection

`gqlListener` extends `BaseGQLListener` and uses `Enter*` callbacks to record raw clause state,
not to compile. Key state fields:

- `vertexPath []vertexSelect` — node patterns in order. Each has a `name` (element variable),
  `label []string`, and `selectMap` (inline property map, raw text values).
- `edgePath []edgeSelect` — edge patterns in order, with `name`, `label []string`, and
  `direction` (`"out"`, `"in"`, `"both"`).
- `returns []string` — raw `RETURN` item text.
- `whereCondition parser.IValueExpressionContext` — the AST node for the WHERE expression (nil if absent).
- `orderExpr`, `skipExpr`, `limitExpr` — raw clause text.
- `unsupported []string` — accumulated unsupported-feature markers; `BuildQuery` fails fast with the joined list.

Re-entry flags drive the pattern handlers, because the grammar has several overlapping production
rules for a single node/edge pattern:

- `inPattern` — set while walking a node or edge pattern; guards `EnterElementVariable`, `EnterLabelName`,
  and `EnterPropertyKeyValuePair`.
- `inRelation` — set while inside an edge pattern; excludes edge properties from `curMap` (edge
  property maps are intentionally unsupported).
- `curVariable`, `curLabels`, `curMap` — scratch accumulators, reset on `EnterNodePattern`/`EnterEdgePattern`.

Direction is set by the `EnterFullEdge*` callbacks: `PointingRight`→`out`, `PointingLeft`→`in`,
`Undirected`→`both`. The hybrid directions (`LeftOrUndirected`, `UndirectedOrRight`, `LeftOrRight`)
mark the query unsupported. `EdgePattern` exit appends the completed `edgeSelect` with the default
direction `"both"`.

### Phase 2: `BuildQuery` assembly

Order of operations for a `MATCH` query (each step only if the clause was present):

1. Validate: `vertexPath` non-empty, and if edges exist `len(vertexPath) == len(edgePath) + 1`
   (rejects disconnected path patterns), no node/edge variable name collisions, no repeated edge
   variables (repeated *node* variables are allowed and tracked in `repeatedVariables`).
2. Emit `V()`, then `addVertexStep` for the first vertex:
   `HasLabel(first label)`, one `Has(Eq(k, v))` per inline-map entry (keys sorted for
   determinism), then `As(name)` — or `SameAs(name)` when the vertex variable repeats earlier
   in the path, instead of `As`.
3. For each edge: `Out/In/Both(label...)` (labels may be empty → unfiltered direction step),
   followed by `addVertexStep` for the next vertex.
4. WHERE: compile via `compileWhereExpression`, require the referenced variable to equal the
   **last** vertex variable (the "current" variable), then emit `Has(predicate.whenTrue)`.
5. ORDER BY: `parseOrderExpression` (requires the same current variable) → `Sort(...)`.
6. RETURN: `parseReturnProjection` per item. A single bare `RETURN var` becomes
   `Render("$var")`; anything else becomes `Render(map[string]any{key: "$var.field"})`.
7. `SKIP`/`LIMIT`: `parseClauseUint` (integer literals only) → `Skip(...)`, `Limit(...)` —
   emitted **after** `Render`.

The emitted step order is significant: `V` → filters/traversal → `Has` (WHERE) → `Sort` → `Render`
→ `Skip` → `Limit`. Preserve it when adding new steps.

### WHERE compilation (three-valued style)

Each predicate compiles to a `compiledWherePredicate{variable, whenTrue, whenFalse}`:

- Comparison `var.f op literal` → `whenTrue = And(Not(Eq(f, nil)), op(f, val))`,
  `whenFalse = And(Not(Eq(f, nil)), Not(op(f, val)))`. The `Not(Eq(f, nil))` guard makes missing
  properties evaluate false on *both* branches.
- `IS NULL` → `whenTrue = Eq(f, nil)`; `IS NOT NULL` → `whenTrue = Not(Eq(f, nil))`.
- `NOT` swaps `whenTrue`/`whenFalse`.
- `AND` combines `whenTrue` with `And(...)` and `whenFalse` with `Or(...)`; `OR` does the reverse
  (De Morgan over the paired branches). `XOR` is rejected.
- Only the `whenTrue` half is emitted into the query; `whenFalse` exists so `NOT` can compose
  correctly. All operands must reference one variable, which must be the current (last) variable.

## Error Conventions

- Unsupported syntax: `fmt.Errorf("unsupported GQL features: ...")` — collected by
  `markUnsupported` during the walk, or returned per-clause in `BuildQuery`. Tests and callers
  rely on this prefix; keep it.
- Syntax errors: `fmt.Errorf("parse error: line L:C ...")` via `syntaxErrorListener`.
- Structural invalidity (bad path shape, etc.): `fmt.Errorf("invalid query: ...")`.

## Known Quirks (behavior to preserve or fix deliberately)

- **Multi-label nodes** like `(n:Person:Known)` are a *parse* error under the current grammar
  production — even though `HasLabel` takes a label list, `addVertexStep` only ever emits the
  first label.
- **Edge labels are optional**: `(n)-[r]->(m)` compiles to an unfiltered `Out()`.
- **String-based clause parsing**: `ORDER BY`, `SKIP`/`LIMIT`, RETURN projections, and simple
  WHERE comparisons are parsed with regexes/text manipulation over `ctx.GetText()` rather than
  walking the AST (the WHERE AST walk handles only boolean composition and `IS [NOT] NULL`).
  `parseClauseUint` strips the clause keyword from the raw text and requires a plain integer.
- **`ORDER BY` with no items** fails with `unsupported GQL features: ORDER BY expression`, but
  `ORDER BY var.field ASC` works; `ASC`/`DESC` are stripped as suffixes, which means a property
  literally named `...desc` would be mis-parsed.
- **`OFFSET` is a grammar synonym of `SKIP`**; both flow through `parseClauseUint`, and
  non-literal values are rejected with a "SKIP expression" error message.
- WHERE/ORDER BY are restricted to the last path variable by design (single-variable traversal
  model), not by accident.
- `EnterFilterStatement` only marks `FILTER` unsupported when the text does not already start
  with `FILTERWHERE`; `FILTER WHERE` currently falls through to a WHERE-compile error.

## Extension Checklist

1. **Add the mapping first** to `GQL_TO_GRIPQL.md` with a GQL example, the expected GripQL
   builder chain, and notes.
2. **Add table-driven tests** in `test/gql_test.go` (`pairs` slice) for the happy path and a
   failing case for anything adjacent that must stay unsupported. Expected queries must match
   `proto.Equal` on every statement, including step order.
3. **Implement in `compiler/build.go`** following the two-phase split: record clause state in
   listener callbacks, compile in `BuildQuery` (or a per-clause helper like
   `parseOrderExpression`). Prefer AST contexts over text parsing where the grammar provides
   them.
4. **Keep unsupported features explicit**: mark them via `markUnsupported` rather than letting
   them fail with generic errors.
5. **Run** `go test ./endpoints/gql/...`.
6. Update `GQL_COMPILER_PROGRESS.md` if the supported/unsupported boundary moved.

## Regenerating the Parser

```sh
curl -O https://www.antlr.org/download/antlr-4.13.2-complete.jar
./generate.sh        # clones opengql/grammar and regenerates parser/
```

Grammar changes in `opengql/grammar` may rename the context types used in `build.go`; after
regenerating, expect compile errors in the listener callbacks and update them accordingly.
