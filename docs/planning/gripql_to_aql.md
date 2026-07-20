# GripQL to AQL Transpiler Specification

## 1. Overview & Architectural Strategy
This document outlines the strategy and technical blueprint for converting GripQL queries into ArangoDB Query Language (AQL).

GripQL operates as an imperative, step-based graph traversal pipeline ($V \rightarrow \text{has} \rightarrow \text{out}$). AQL is a declarative, functional language leveraging nested loops ($\text{FOR} \rightarrow \text{FILTER} \rightarrow \text{RETURN}$). The transpiler bridges this gap by tracking active scope variables and mapping the imperative sequence to nested AQL constructs.

The transpiler processes a sequence of `GraphStatement` protobuf messages from a `GraphQuery`, translating each operation into its AQL equivalent.

## 2. Translation Mapping & Reference

### 2.1 Vertex Selection (`v`)
- **Protobuf**: `GraphStatement.v` (ListValue)
- **AQL**: 
  - Empty/Generic: `FOR v IN Vertices`
  - With IDs: `FOR v IN [DOCUMENT("Vertices", "id1"), ...]`

### 2.2 Filtering (`has`, `has_label`, `has_key`, `has_id`)
| Protobuf Field | GripQL Syntax | AQL Equivalent | Example (Protobuf) | Example (AQL) |
| :--- | :--- | :--- | :--- | :--- |
| `has` | `.has(GripQL.eq(...))` | `FILTER v.<expr>` | `HasExpression(condition: EQ, key: "symbol", value: "TP53")` | `FILTER v.symbol == "TP53"` |
| `has_label` | `.hasLabel("...")` | `FILTER v.label IN [...]` | `ListValue(["Protein"])` | `FILTER v.label IN ["Protein"]` |
| `has_id` | - | `FILTER v._id IN [...]` | `ListValue(["Vertices/123"])` | `FILTER v._id IN ["Vertices/123"]` |

**Compound Conditionals:**
GripQL logic operators map directly to AQL boolean filters:
- `GripQL.eq(...)` $\rightarrow$ `==`
- `GripQL.gt(...)` $\rightarrow$ `>`
- `GripQL.gte(...)` $\rightarrow$ `>=`
- `GripQL.and(...)` $\rightarrow$ `AND`
- `GripQL.or(...)` $\rightarrow$ `OR`
*Composite multi-conditional criteria should be wrapped in bracket expressions `( ... )` to ensure operator precedence.*

### 2.3 Traversal (`out`, `in`, `both`)
- **Protobuf**: `GraphStatement.out` / `in` / `both` (ListValue containing edge labels/filters)
- **AQL**: Use AQL traversals: `FOR v, e, p IN 1..1 OUTBOUND current_vertex GraphName`.
- **Edge Focus (`out_e`, etc.)**: Return `e` instead of `v` in the RETURN statement.
- **Multi-step Traversals**: Translated by chaining nested `FOR` loops. Each traversal step creates a new loop level, and intermediate `.has()` filters are placed as `FILTER` statements within that level.

### 2.4 Memory & Iteration (`mark`, `jump`, `set`, `increment`)
- **Protobuf**: `GraphStatement.mark`, `jump`, `set`, `increment`.
- **AQL**: 
  - `set`/`increment`: Use AQL variables or temporary collections/properties if complex.
  - `mark`/`jump`: Map to variable-depth traversals (`1..N`) where possible, or decompose into multiple queries for conditional loops.

### 2.5 Terminal Modifiers & Output
Terminal steps rewrite the structural output rather than appending loops:
- **Count**: `.count()` $\rightarrow$ Wrap interior traversal in `RETURN LENGTH( <nested_loops> )`.
- **Render**: Server-side projection applied after AQL results are returned (see Section 4).
- **Limit/Pagination**: `.limit(10)` $\rightarrow$ Add `LIMIT 10` before the RETURN statement.

### 2.6 Named Scoping & Selection (`as`, `select`)
Named scoping allows vertices/edges to be tagged with identifiers for later retrieval across pipeline hops.

| Protobuf Field | GripQL Syntax | AQL Mechanism |
| :--- | :--- | :--- |
| `alias` (string) | `.as("name")` | Register current scope variable under user-defined name in state tracker |
| `select_name` (string) | `.select("name")` | Emit reference to previously tagged scope variable in RETURN expression |

**Variable Lifecycle:**
1. `.as("a")` anchors the active vertex/edge to the named variable `a`, stored in a scope map `{ "a": v0 }`.
2. Subsequent traversals create fresh loop variables (e.g., `v1`, `e1`), but `a` remains accessible.
3. `.select("a")` resolves from the scope map and emits `$a` or `@@scope.a` in the RETURN expression.
4. Scopes persist until cleared or overwritten by a new `.as()` with the same name.

**AQL Variable Naming Convention:**
Named scopes use the prefix `$` in render/SELECT expressions:
- `.as("a")` registered → stored as `$a` → reference via `@@named_scopes["a"]`.
- Multiple aliases on same vertex: later alias overwrites previous mapping.

**Deep Chaining Example:**
```
G.V("Character:1").as_("a").out().as_("b").out().as_("c")
```
AQL state tracker:
| Step | Active Loop Var | Named Scopes |
|------|-----------------|--------------|
| Start | — | {} |
| `.as("a")` | `v0` | `{ "a": v0 }` |
| `.out()` | `v1, e1` (OUTBOUND v0) | `{ "a": v0 }` |
| `.as("b")` | `v1` | `{ "a": v0, "b": v1 }` |
| `.out()` | `v2, e2` (OUTBOUND v1) | `{ "a": v0, "b": v1 }` |
| `.as("c")` | `v2` | `{ "a": v0, "b": v1, "c": v2 }` |

### 2.7 Path Reconstruction (`.path()`)
The `.path()` terminal step returns the complete traversal history as a sequence of `{vertex: ..., edge: ...}` tuples per hop. The path includes both vertex and edge objects for every hop, starting with the initial vertex.

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.path()` | Use AQL traversal path object `p` — `{ vertices: [...], edges: [...] }` from `FOR v, e, p IN ... TRAVERSAL` |

**Output Format:**
Each result row is an array of hop objects. Even-numbered indices (0, 2, 4...) are vertex hops; odd indices (1, 3, 5...) are edge hops:
```json
[
  { "vertex": { "_id": "Film:1", ... } },
  { "edge": { "_id": "Film:1/planets/1", "_label": "planets", ... } },
  { "vertex": { "_id": "Planet:1", ... } }
]
```

**With Named Scope Select:**
`.as("a").outE().as_("b").select("b").path()` — path with intermediate edge selection. The SELECT clause includes `$b` (the edge) while the PATH returns full hop history.

**Edge-Only Paths (`.outE()`, `.inE()`):**
When edge-only traversals are used, odd-indexed hops contain edge documents and even indices may omit vertex data depending on whether `outE()` or `inE()` is chained with `.out()/in_()`.

### 2.8 Deduplication (`.distinct()`)
The `.distinct()` terminal step produces unique values across the result set, optionally scoped to a specific field or named scope variable.

| GripQL Syntax | AQL Equivalent | Behavior |
| :--- | :--- | :--- |
| `.distinct()` | `RETURN DISTINCT v` | Deduplicate full document objects |
| `.distinct("field")` | `RETURN DISTINCT v.field` | Deduplicate by single field value |
| `.distinct("$scope.field")` | Resolve scope → `RETURN DISTINCT @@scopes["scope"].field` | Multi-scope cross-reference deduplication |
| `.distinct(["$a.f", "_id"])` | `RETURN DISTINCT [@@scopes["a"].f, v._id]` | Deduplicate by composite key (array of expressions) |

**Implementation:**
- No field → pass through AQL `DISTINCT` modifier on RETURN.
- Single field → `RETURN DISTINCT current_vertex.field`.
- Scoped variable like `$person.name` → resolve scope map to get anchor vertex, emit `RETURN DISTINCT @@named_scopes["person"].name`.
- Multi-field array → construct `RETURN DISTINCT [expr1, expr2]` with proper scope resolution for each.

### 2.9 Sorting (`.sort()`)
The `.sort()` step produces ordered results within the AQL RETURN expression.

| GripQL Syntax | AQL Equivalent |
| :--- | :--- |
| `.sort("field")` | `SORT v.field ASC` |
| `.sort("field", descending=True)` | `SORT v.field DESC` |
| Multi-field: `.sort([("f1", "asc"), ("f2", "desc")])` | `SORT v.f1 ASC, v.f2 DESC` |

**Placement in Query:**
AQL `SORT` clauses are placed immediately before the RETURN statement at the appropriate nesting level (innermost loop for scoped sorts, outermost for top-level sorts). Sort on named scope variables resolves to the registered anchor vertex variable.

### 2.10 Aggregation — Group (`.group()`)
The `.group()` step groups results into map-keyed collections. The key is typically the current vertex's identifier, and the values are collected from named scope variables.

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.group({"key": "$scope.field"})` | AQL `COLLECT` with grouping by a field; accumulate into arrays per group key |
| `.group({"k1": "$s1.f", "k2": "$s2.f"})` | Multi-field accumulation: each result row maps to `{ k1: [...], k2: [...] }` per group |

**Example Translation:**
```
G.V().hasLabel("Planet").as_("planet").out("residents").as_("character")
  .select("planet").group({"people": "$character.name"})
```

AQL:
```aql
FOR v0 IN [FILTER v0._label == "Planet"]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.label == "residents"
COLLECT key = v0.name INTO groups = DISTINCT v1.name
RETURN { name: key, people: groups }
```

### 2.11 Aggregation — Aggregate (`.aggregate()`)
The `.aggregate()` step produces named count aggregations over field values. Two variants are used in the conformance tests:

**Term Aggregation:** `aggregate(gripql.term(name, field))`
- Counts occurrences of each unique value for the specified field.
- Each result row has: `name` (aggregation name), `key` (unique value), `value` (count), and optionally `result_name`.

```
G.V().aggregate(gripql.term("simple-agg", "eye_color"))
```
AQL:
```aql
FOR v IN Vertices
  FILTER IS_NULL(v.eye_color) == false
COLLECT key = v.eye_color INTO count = 1
RETURN { name: "simple-agg", key: key, value: LENGTH(count) }
```

**Histogram Aggregation:** `aggregate(gripql.histogram(name, field, bucket_size))`
- Groups values into numeric bins of configurable width.
- `key` = bin midpoint; `value` = count in that bin.

### 2.12 Unpivot (`.pivot()`)
The `.pivot()` step unpivots nested key-value results into flat tabular rows. It accepts a list of field references (scalars, scoped variables).

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.pivot("$a._id", "$.key", "$.value")` | For each result with nested `key`/`value` fields, emit one row per pair with the scoped identifiers |

**Example:**
```
G.V().hasLabel("Patient").as_("a").out("patient_observation")
  .pivot("$a._id", "$_key", "$_value")
```
AQL — flatten nested observation key-value pairs:
```aql
FOR v0 IN [FILTER v0._label == "Patient"] AS patient
  FOR v1, e1 IN 1..1 OUTBOUND patient GraphName
    FILTER e1.label == "patient_observation"
    RETURN { _id: patient._id, key: v1._key, value: v1 }
```

### 2.13 Unwind (`.unwind()`)
The `.unwind()` step unpacks array fields into separate records — one row per array element. Chained unwinding supports deeply nested arrays.

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.unwind("field")` | AQL `FOR item IN v.field ... RETURN MERGE(v, { unpacked: item })` (one row per array element) |
| `.unwind("a.b.coding")` | Nested dot-path unwind: iterate through nested arrays at the specified path |

**Null-Entry Handling:**
Unwinding must gracefully skip null entries produced by `outNull()` traversals — filter out documents where the target field is not an array or is null before unwinding.

**Example:**
```
G.V().hasLabel("Observation").unwind("component")
```
AQL:
```aql
FOR v IN Observation
  FOR comp IN IS_NULL(v.component) ? [] : v.component
    RETURN MERGE(v, { "unwound_component": comp })
```

### 2.14 Null Connection Traversal (`outNull`, `inNull`, `bothNull`)
These operations extend standard traversal by producing **null entries** for source nodes that have no connections via the specified edge label — enabling "find entities without X" queries.

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.outNull("edgeLabel")` | Standard OUTBOUND traversal + UNION with self-referencing null placeholder for disconnected sources |
| `.inNull("edgeLabel")` | Standard INBOUND traversal + null placeholder |
| `.bothNull("edgeLabel")` | Bidirectional traversal + null placeholder |

**AQL Pattern:**
```aql
// Part 1: Real connections
FOR v0, e0, p0 IN 1..1 OUTBOUND source GraphName
  FILTER e0.label == "target_edge"
  RETURN { _id: v0._id, edge_data: e0 }

UNION

// Part 2: Null entry for disconnected nodes
FOR v_disc IN (
  FOR s IN AllNodes
    FILTER !LENGTH(
      (FOR e IN Edges FILTER e._to == s._id && e.label == "target_edge" RETURN e)
    )
    RETURN s
)
RETURN { _id: v_disc._id, null_connection: true }
```

### 2.15 Edge-Only Output (`.outE()`, `.inE()`)
Edge-only traversal operators return edge documents directly as results instead of vertex targets.

| GripQL Syntax | AQL Mechanism |
| :--- | :--- |
| `.outE(["edgeLabel"])` | `FOR e IN Edges FILTER e._from == currentVertex RETURN e` |
| `.inE(["edgeLabel"])` | `FOR e IN Edges FILTER e._to == currentVertex RETURN e` |

**With Named Scope:**
`.as_("a").outE().as_("b")` — anchors both the starting vertex (`$a`) and the edge document (`$b`). In RETURN, `$a` resolves to the source vertex and `$b` to the edge.

### 2.16 Field Projection (`.fields()`)
The `.fields([...])` step filters which fields are included in the AQL RETURN expression itself (server-side projection at query time, not post-processing).

| GripQL Syntax | AQL Equivalent |
| :--- | :--- |
| `.fields(["name"])` | `RETURN MERGE(ONLY(@@active_vertex), { _id: @@active_vertex._id, _label: @@active_vertex._label }) WITH ONLY "name"` |

**Implementation:**
- `_id` and `_label` are always included (GripQL metadata requirement).
- Additional fields are projected via `RETURN MERGE(v, { ... })` with only the requested fields copied.
- Non-existent fields are silently omitted from projection (per test: querying for `"non-existent"` still returns just `_id` and `_label`).

### 2.17 Type Coercion (`.totype()`)
The `.totype()` step performs runtime type conversion on vertex/edge properties. Supports nested path access via dot notation.

| GripQL Syntax | Supported Types | Fallback Behavior |
| :--- | :--- | :--- |
| `.totype("field", "string")` | `string`, `float`, `int`, `list`, `bool` | null → default (0, "", [], false) |
| `.totype("nested.field", "int")` | Same types; dot-path navigation into nested objects | Unparseable string → 0 |

**AQL Mapping:**
```aql
// totype("birth_year", "string")
CONVERT_TO_STRING(v.birth_year)

// totype("mass", "float")  
TO_NUMBER(v.mass) // fallback 0.0 if null/non-numeric

// totype("system.created", "bool")
v.system.created == true ? true : false  // nested access with null safety
```

**Chaining:** Multiple `.totype()` calls chain sequentially, each transforming the current result row's fields.

### 2.18 Composite hasLabel (Array)
The `.hasLabel(["A", "B"])` variant filters vertices matching any of multiple labels simultaneously.

| GripQL Syntax | AQL Equivalent |
| :--- | :--- |
| `.hasLabel("SingleLabel")` | `FILTER v._label == "SingleLabel"` |
| `.hasLabel(["Vehicle", "Starship"])` | `FILTER v._label IN ["Vehicle", "Starship"]` |

### 2.19 Index Operations
Index management is a **schema-level operation** rather than a query step. The transpiler does not translate index operations into AQL — these are handled by the GripQL schema layer:

| Operation | Description |
|-----------|-------------|
| `addIndex(label, field)` | Creates an index on `label.field` in ArangoDB (direct DB command) |
| `deleteIndex(label, field)` | Drops the named index |
| `listIndices()` | Returns all indices for the graph |

Indexed fields can be used as optimization hints in the transpiler: queries matching `has(gripql.eq("indexed_field", value))` should leverage AQL `INDEX_HINT` directives.

### 2.20 Duplicate & Identity Resolution
GripQL allows multiple vertex additions with the same ID. The KVGraph layer deduplicates by (label, id) tuples before query execution:

| Scenario | Behavior |
|----------|----------|
| Same ID + same label → second add replaces first | Vertex data merged; only final state visible to queries |
| Same ID + different label → both stored as merged object | `G.V().count()` counts unique IDs (not document count) |
| Edge duplicates on same pair | Last write wins for edge metadata |

## 3. Examples

### 3.1 Simple Filtered Lookup
**Protobuf Sequence:**
1. `v: []`
2. `has: { condition: EQ, key: "symbol", value: "TP53" }`

**AQL:**
```aql
FOR v IN Vertices
  FILTER v.symbol == "TP53"
  RETURN v
```

### 3.2 One-step Traversal with Filter
**Protobuf Sequence:**
1. `v: ["Vertices/Character:1"]`
2. `out: ["FriendEdge"]`
3. `has: { condition: EQ, key: "name", value: "Friend" }`

**AQL:**
```aql
FOR v, e IN 1..1 OUTBOUND DOCUMENT("Vertices", "Character:1") GraphName
  FILTER e.label == "FriendEdge" AND v.name == "Friend"
  RETURN v
```

### 3.3 Multi-step Traversal
**Protobuf Sequence:**
1. `v: ["Vertices/Character:1"]`
2. `out: ["edge1"]`
3. `has: { condition: EQ, key: "color", value: "blue" }`
4. `out: ["edge2"]`

**AQL:**
```aql
FOR v0 IN [DOCUMENT("Vertices", "Character:1")]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.label == "edge1"
    FILTER v1.color == "blue"
    FOR v2, e2 IN 1..1 OUTBOUND v1 GraphName
      FILTER e2.label == "edge2"
      RETURN v2
```

### 3.4 Multi-step Traversal with Render
**Protobuf Sequence:**
1. `v: ["Vertices/Character:1"]`
2. `out: ["edge1"]`
3. `has: { condition: EQ, key: "color", value: "blue" }`
4. `out: ["edge2"]`
5. `render: ["name", "age"]`

**AQL (same as Case 3 — render is NOT translated):**
```aql
FOR v0 IN [DOCUMENT("Vertices", "Character:1")]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.label == "edge1"
    FILTER v1.color == "blue"
    FOR v2, e2 IN 1..1 OUTBOUND v1 GraphName
      FILTER e2.label == "edge2"
      RETURN v2
```

**Server-side render projection (post-AQL):**
After AQL returns complete vertex documents, the server applies `render(["name", "age"])`:
```json
// Raw AQL result
{ "_id": "Vertices/Character:5", "name": "Alice", "age": 42, "role": "admin" }

// After render(["name", "age"])
{ "name": "Alice", "age": 42 }
```

### 3.5 Named Scoping & Path Output
**Protobuf Sequence:**
1. `v: ["Vertices/Character:1"]`
2. `as: "a"`
3. `out: []`
4. `as: "b"`
5. `out: []`
6. `as: "c"`
7. `path: true`
8. `render: {"a": "$a", "b": "$b", "c": "$c"}`

**AQL:**
```aql
FOR v0 IN [DOCUMENT("Vertices", "Character:1")]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FOR v2, e2 IN 1..1 OUTBOUND v1 GraphName
      RETURN {
        path: [
          { vertex: @@named_scopes["a"] },
          { edge: e1 },
          { vertex: @@named_scopes["b"] },
          { edge: e2 },
          { vertex: @@named_scopes["c"] }
        ],
        rendered: {
          a: @@named_scopes["a"],
          b: @@named_scopes["b"],
          c: @@named_scopes["c"]
        }
      }
```

### 3.6 Distinct Deduplication (scoped field)
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Character"]`
3. `as: "person"`
4. `out: ["homeworld"]`
5. `distinct: "$person.eye_color"`

**AQL:**
```aql
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    RETURN DISTINCT @@named_scopes["person"].eye_color
```

### 3.7 Sort with Limit
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Character"]`
3. `sort: { field: "height", descending: true }`
4. `limit: 10`

**AQL:**
```aql
FOR v IN Vertices
  FILTER v._label == "Character"
  SORT v.height DESC
  LIMIT 10
  RETURN v
```

### 3.8 Group Aggregation
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Planet"]`
3. `as: "planet"`
4. `out: ["residents"]`
5. `as: "character"`
6. `select: "planet"`
7. `group: { people: "$character.name" }`

**AQL:**
```aql
FOR v0 IN Vertices
  FILTER v0._label == "Planet"
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.label == "residents"
COLLECT planet_id = v0._id INTO groups = DISTINCT v1.name
RETURN { name: planet_id, people: groups }
```

### 3.9 Unwind (array unpacking)
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Observation"]`
3. `unwind: "component"`

**AQL:**
```aql
FOR v IN Observation
  FOR comp IN IS_NULL(v.component) ? [] : v.component
    RETURN MERGE(v, { unwound_component: comp })
```

### 3.10 Null Connection Traversal (find missing edges)
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Character"]`
3. `out_null: "starships"`
4. `render: ["_id", "name"]`

**AQL:**
```aql
// Connected characters (return nothing for these)
FOR v0 IN Vertices
  FILTER v0._label == "Character"
    AND LENGTH(
      (FOR e IN Edges FILTER e._from == v0._id && e.label == "starships" RETURN e)
    ) > 0
    RETURN NULL

UNION

// Disconnected characters (null connection entries)
FOR v0 IN Vertices
  FILTER v0._label == "Character"
    AND LENGTH(
      (FOR e IN Edges FILTER e._from == v0._id && e.label == "starships" RETURN e)
    ) == 0
    RETURN MERGE(v0, { null_connection: true })
```

### 3.11 Edge-Only Traversal with Path
**Protobuf Sequence:**
1. `v: ["Vertices/Film:1"]`
2. `as: "a"`
3. `out_e: ["planets"]`
4. `as: "b"`
5. `out: []`
6. `select: "b"`
7. `path: true`

**AQL:**
```aql
FOR v0 IN [DOCUMENT("Vertices", "Film:1")]
  FOR e1 IN Edges
    FILTER e1._from == v0._id && e1.label == "planets"
    LET edge_anchor = @@named_scopes["b"] := e1
    FOR v2, e2 IN 1..1 OUTBOUND (e1)._to GraphName
      RETURN {
        path: [
          { vertex: @@named_scopes["a"] },
          { edge: @@named_scopes["b"] },
          { vertex: v2 }
        ]
      }
```

### 3.12 Type Coercion Pipeline
**Protobuf Sequence:**
1. `v: []`
2. `has_label: ["Character"]`
3. `totype: [{ field: "birth_year", type: "string" }, { field: "mass", type: "float" }, { field: "system.created", type: "bool" }]`
4. `render: ["name", "birth_year", "mass"]`

**AQL:** (totype is applied server-side post-AQL)
```aql
FOR v IN Vertices
  FILTER v._label == "Character"
  RETURN v
```

**Post-AQL transform per row:**
```python
# Server-side coercion after AQL fetches documents
row["birth_year"] = str(row.get("birth_year", ""))   # null → ""
row["mass"] = float(row.get("mass", 0))               # null/non-numeric → 0.0
row["system"]["created"] = bool(row["system"]["created"])  # nested with null safety
```

The `.render(fields)` method is a **terminal step** in the GripQL pipeline that controls how results are formatted and returned to the client. Rather than encoding this logic into AQL, the render operation is applied **server-side after** the AQL query completes. This design has several advantages:

## 4. Render Step Details

### 4.1 Role in the Pipeline
- Render always appears as the **last step** in a GripQL pipeline — it terminates traversal and defines output shape.
- It does not affect traversal logic, filtering, or vertex/edge selection. The underlying query returns complete vertex documents (or edge documents if focused on edges) from AQL.
- After AQL returns its result set, the server applies a lightweight field projection to include only the requested attributes in the response.

### 4.2 Implementation Strategy
Render is implemented entirely on the GripQL server side:

1. **Generate AQL query** as normal — traversal loops produce complete document objects (`v` or `e`).
2. **Execute AQL query** against ArangoDB and receive full documents in the response body.
3. **Apply projection** in the server's post-processing layer, retaining only the fields specified in `.render(...)`.

This approach avoids:
- Embedding field projections inside AQL `RETURN` expressions (which would require transpiling each field mapping into every pipeline path).
- Maintaining parallel field-mapping logic in the AQL generator and the render handler.
- Regenerating AQL whenever render fields change mid-pipeline.

### 4.3 Example Walkthrough

**GripQL Pipeline:**
```
v(["Vertices/Protein:1"])
  .out("InteractsWith")
  .has(GripQL.eq("confidence", 0.95))
  .render(["id", "name", "confidence"])
```

**Step 1 — Transpile to AQL (ignoring render):**
```aql
FOR v0 IN [DOCUMENT("Vertices", "Protein:1")]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.confidence == 0.95
    RETURN v1
```

**Step 2 — Execute AQL and receive results:**
```json
[
  { "_id": "Vertices/Protein:2", "id": "Protein:2", "name": "BRCA1", "confidence": 0.97, "type": "protein" },
  { "_id": "Vertices/Protein:3", "id": "Protein:3", "name": "EGFR", "confidence": 0.96, "type": "protein" }
]
```

**Step 3 — Server-side render projection:**
For each result document, keep only `["id", "name", "confidence"]`:
```json
[
  { "id": "Protein:2", "name": "BRCA1", "confidence": 0.97 },
  { "id": "Protein:3", "name": "EGFR", "confidence": 0.96 }
]
```

### 4.4 Key Properties
| Property | Description |
|---|---|
| **Position** | Terminal — must be the last step in any pipeline that produces output. |
| **Scope** | Accepts a list of field names; only those fields are included in the final response. |
| **Omission** | If `.render()` is omitted, the server returns full documents as produced by AQL (default behavior). |
| **Nesting** | If render fields contain nested objects or arrays, the server may optionally flatten them or return them as-is depending on configuration. |
| **Null handling** | Fields that exist in the document but are `null` are included; missing fields are excluded from the projected output. |

### 4.5 Interaction with Other Terminal Modifiers
- Render can coexist with `.limit(N)` — AQL applies the limit, then the server renders the remaining results.
- Render and `.count()` are mutually exclusive outputs: count returns a scalar integer, while render returns an array of projected objects. If both appear in a pipeline, the one that appears last takes precedence for the output format.

## 5. Implementation Instructions

### 5.1 Stateful Scope & Variable Allocation
GripQL relies on an implicit data stream. AQL requires explicit symbols. Implement a monotonically increasing variable scope allocator (e.g., `v1`, `v2`, `v3`). Every traversal step (`.out()`, `.in_()`) must use the currently active vertex variable as the anchor and generate a new isolated identifier tuple `(v_n, e_n, p_n)` for the nested loop block.

### 5.2 Deterministic Document ID Formatting
ArangoDB requires `"CollectionName/document_key"` syntax. The parser must look back at `.hasLabel("LabelName")` context flags to resolve the proper collection route dynamically when mapping root `V()` initialization calls.

### 5.3 Pipeline Components
1. **Input Handler**: Deserialize JSON/Protobuf `GraphQuery` into a usable internal representation of `GraphStatement` sequences.
2. **Mapper**: Map each `GraphStatement` type to AQL constructs (FOR, FILTER, etc.) using the reference mapping in Section 2.
3. **State Tracker**: Maintain current context (active vertex variable, scope allocator, and variables for `set`/`increment`).
4. **Generator**: Assemble mapped components into a final AQL string, applying terminal decorators (count, render) as outer wraps.
5. **Validator**: Compare results of JSON-defined GripQL queries against their generated AQL output on an ArangoDB instance.
