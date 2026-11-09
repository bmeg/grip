# GRIPPER v2 — Federated Traversal Plan

Status: design agreed with @kellrott (decisions in §2). Breaking change: no backward compatibility with
the v1 collection API. First implementation step (WP1) should copy §1–§9 of this document into the repo as
`gripper/DESIGN.md` so implementation agents can read it without access to the session folder.

---------------------------------------------------------------------------------------------------

## 1. Problem & approach

Today (`gripper/`): a `GRIPSource` is a dumb collection server (`GetRows`, `GetRowsByID`, `GetRowsByField`, ...).
`TabularGraph` (gripper/graph.go) implements `gdbi.GraphInterface` on top of it, doing vertex/edge mapping
(`vertices:` / `edges:` mapping YAML, `fieldToField`/`edgeTable`) *centrally* and issuing one RPC per
traveler per hop. Slow and chatty.

New model:

* A **GRIPSource** (resource node) is a *subgraph* whose vertex AND edge IDs all start with one CURIE prefix
  (e.g. `Character:`). It runs GripQL traversal steps locally.
* Edges may point to vertices owned by other sources (foreign prefix).
* The **central processor** (`gripper/`, replaces `TabularGraph`) compiles a GripQL query, cuts it into
  *segments* of pushable steps, streams travelers to the right sources over a bidirectional `Traverse` RPC,
  merges results, re-routes travelers that hit a foreign prefix, and runs non-pushable steps itself.
* Resource nodes are written in Go using library code in this repo (`gripper/node/...`). Python examples are deleted.

Key reuse insight: the node-side executor is just `engine/core`'s existing compiler/processors run over a
*boundary-aware wrapper* of any `gdbi.GraphInterface`. `CompileOptions.Extends` (gdbi/pipeline.go) already lets
a pipeline start mid-query (used by job Resume), which is what we need for "enter at step k".
Likewise `FederatedGraph` (central) implements `gdbi.GraphInterface` itself, so *the unmodified core engine already
works against it* (baseline, WP5) and the segment compiler (WP6) is purely an optimization on top.

## 2. Decisions (confirmed)

| # | Decision |
|---|----------|
| D1 | Cross-source reverse edges: sources declare in `GetInfo` which foreign prefixes/labels they hold edges *into*; central fans `in/inE/both/bothE` out to the owner **plus every declaring source**. |
| D2 | Central owns full traveler state (Marks, Path, Selections, Count…) in a `requestID`-keyed table. Wire carries `{requestID, cursor, current}` in and *deltas* (new current, marks added, path added) out. |
| D3 | Node library = (a) generic `TraversalServer` wrapping **any** `gdbi.GraphInterface` (kvgraph/sqlite/mongo/psql…) and (b) native table/collection-mapping adapter (replaces Python proxies). |
| D4 | Dangling cross-source edges (unknown prefix, or owner lacks the vertex) → traveler silently dropped + `log.Warn` (rate-limited). |
| D5 | Remove: old collection RPCs, Python gripper package/examples, `__mapping__` graph mechanism, `ListTables`/`TableInfo` API + python `listTables`, mapping-YAML gripper config. **Keep** go-plugin launching (`StartPlugin`/`ListDrivers`) for Go resource-node binaries (`gripper-<name>`). |
| D6 | v1 pushdown set: `V(ids/labels)`, `hasLabel`, `hasId`, `hasKey`, `has` (no references to marks created outside the segment), `in/out/both`, `inE/outE/bothE`, `*Null` variants, `as`. Everything else (limit, skip, range, count, distinct, sort, group, pivot, select, fields, render, path-render, aggregate, totype, unwind, set/increment, **mark/jump**) runs centrally. |
| D7 | Test strategy: Go integration tests over bufconn **and** the full existing conformance suite against a federated SWAPI where **each label is its own source** (`Character:`, `Planet:`, `Film:`, … prefixes already match) so every edge is a cross-source edge. |

## 3. Wire protocol (`gripper/gripper.proto`, rewritten)

```proto
service GRIPSource {
  rpc GetInfo(Empty) returns (SourceInfo);
  rpc Traverse(stream TraverseRequest) returns (stream TraverseResponse);   // everything else is a traversal
}

message SourceInfo {
  uint32 protocol_version = 1;
  string name        = 2;
  string prefix      = 3;               // e.g. "Character:"  (all vertex+edge IDs start with it)
  gripql.Graph schema = 4;              // vertex labels, edge labels + from/to labels, fields (for BuildSchema + pruning)
  repeated EdgeLink edge_links = 5;     // edges stored here that cross prefixes
  repeated string indexed_fields = 6;   // "label.field" (optional; for has-pushdown planning)
  uint32 max_batch = 7;
}
message EdgeLink { string label = 1; string from_prefix = 2; string to_prefix = 3; }  // inbound-index for D1

message Element { string id=1; string label=2; string from=3; string to=4; google.protobuf.Struct data=5; bool loaded=6; }

message TraverseRequest {
  oneof msg {
    Start start = 1;          // exactly once, first message
    TravelerBatch batch = 2;
    Flush flush = 3;          // barrier
  }
}
message Start {
  repeated gripql.GraphStatement steps = 1;   // the fixed pushable segment (central cuts it; D6)
  repeated uint32 entry_cursors = 2;          // cursors at which central may inject travelers (0 = seeds)
  bool load_final = 3;                        // central needs data of the segment's final element
  // central-computed hints per step; see §6
  repeated bool probe_foreign = 4;            // step i is an in/both-type step entered via fan-out
}
message TravelerIn {
  uint64 request_id = 1;
  uint32 cursor     = 2;      // index of NEXT step to run. 0 + no current = seed (V() scan)
  Element current   = 3;      // id always set; data only if known
  bool   needs_resolve = 4;   // current is only an id: owner must load it; if missing -> drop (D4)
  bool   foreign_probe = 5;   // run an in/both step for a vertex NOT owned here (fan-out, D1); skip resolve
}
message TravelerBatch { repeated TravelerIn travelers = 1; }
message Flush { uint64 id = 1; }

message TraverseResponse {
  oneof msg {
    ResultBatch results = 1;
    FlushAck ack = 2;
    Error error = 3;
  }
}
message TravelerOut {
  uint64 request_id = 1;             // links to the ORIGINAL TravelerIn (many outs per in; D2)
  uint32 cursor     = 2;             // == len(steps): segment complete.  < len(steps): HANDOFF (see §5)
  Element current   = 3;             // may be a bare id for HANDOFF
  map<string, Element> marks = 4;    // marks created inside the segment (delta)
  repeated PathElem path = 5;        // path entries added inside the segment (delta)
  bool null_move    = 6;             // *Null step found nothing (see §5.3)
}
message ResultBatch { repeated TravelerOut results = 1; }
message FlushAck { uint64 id = 1; }
```
Message names/numbers are a draft; WP1 owns the final form. Requirements that must survive: batching in both
directions (chattiness), `request_id` on every output, `cursor` semantics, delta marks/path, flush/ack barrier.

Traveler fields **not** sent: Selections, Aggregation, Count, Render, Signal (central-only / pushdown-excluded).

## 4. Components & file layout

```
gripper/
  gripper.proto + *.pb.go       WP1   (make proto; Makefile target already exists)
  DESIGN.md                     WP1   (copy of this plan §1–§9)
  wire.go                       WP2   Element/Traveler <-> proto conversion
  registry.go                   WP3   sources, GetInfo cache, prefix->source (longest match), inbound index
  federation.go (FederatedGraph) WP5  gdbi.GraphInterface via single-step Traverse calls  [replaces graph.go, graphdb.go, optimize.go, schema.go]
  planner.go                    WP6   segmenter: []GraphStatement -> []Segment | central step
  segment.go                    WP6   segment gdbi.Processor (state table, routing, handoff loop, termination)
  stream.go                     WP6   per-(segment,source) Traverse stream: batching sender/reader, unbounded handoff queue
  node/
    server.go                   WP4   GRIPSourceServer impl wrapping gdbi.GraphInterface + prefix
    session.go                  WP4   per-Traverse-stream: entry pipelines, egress, flush
    boundary.go                 WP4   BoundaryGraph (gdbi.GraphInterface wrapper) 
    procs.go                    WP4   Resolve + Divert processors
    info.go                     WP4   GetInfo from inner schema
    table/                      WP7   native collection-mapping adapter (Driver iface, mapping config, GraphInterface impl)
gdbi/traveler.go                WP2   exported ApplyDelta helper
server/, config/, cmd/          WP3/WP8/WP10 wiring + removal of legacy
```
Deleted in WP10: `gripper/{graph,graphdb,optimize,schema,channel_mux,queue,driver_cache,server,client}.go` (client/driver
cache superseded), `gripper/python/`, `gripper/gdc-graph/`, `gripper/test-graph/`, `gripper/schema.go~`,
`cmd/erclient/` (or rewrite), website docs under `website/content/docs/gripper*`.

## 5. Semantics (the part that must be exact)

### 5.1 Cursor & segments
A segment = a contiguous run of pushable steps `steps[0..n)`. A traveler's `cursor` k is the index of the next step to run.
Central assigns every traveler a `request_id`, stores the full `BaseTraveler` in `state[request_id]`, and sends
`{request_id, cursor, current(id, data if loaded)}`.  Outputs reference the same `request_id`; central rebuilds
descendants as `state[rid].Copy()` + `ApplyDelta(current, marks, path)` (WP2).
Seeds: `V()` is not per-traveler; central sends **one seed traveler** (cursor 0, no current) to each candidate source
(pruned by `V(ids)` prefix and `hasLabel` vs `SourceInfo.schema`).

### 5.2 The three outcomes of a traveler at a source
1. **Descendants** — traveler moved to new elements (cloned per outgoing edge) and either finished
   (`cursor == len(steps)` → RESULT) or…
2. **Eliminated** — failed a filter / no edge: *nothing is emitted* (accounted for only via Flush, §5.4).
   Exception: `*Null` steps, see §5.3.
3. **Handoff** — the move step reached an element whose ID prefix ≠ node prefix. The node does **not** look it up;
   it emits `TravelerOut{cursor = k+1 (step after the move), current = {id: foreignId, loaded:false}}`. Central routes it by
   prefix to the owner with `needs_resolve=true`; owner loads the vertex (missing ⇒ drop, D4) and continues at `cursor`.
   If `cursor == len(steps)` the owner still resolves and returns it as RESULT (existence check).
   Edge→vertex adjacency (`outE` then `out`) is the same mechanism (`GetVertexChannel` on a foreign `edge.To`).

### 5.3 `*Null` steps (`outNull`, `inNull`, `outENull`, `inENull`)
* Owner-only steps (`outNull`/`outENull`, and `in*Null` with no foreign inbound candidates): node emits a normal null traveler
  (`IsNull()`; `null_move=true`) which the core engine already handles; central converts to a null `BaseTraveler`.
* Fan-out steps (`inNull`/`inENull` with D1 candidates): the step is forced to be the **first step of its segment**
  (§5.5); central dispatches to N sources, each reports children or `null_move`; central emits **one** null traveler iff all N reported
  `null_move` and none produced a child/handoff. Aggregation state lives in `state[request_id].nullPending`.
* Dangling cross-source edges are *not* "null" (D4).
* Existing quirk to preserve (don't "fix"): after an edge-typed current, `inNull/outNull` ignore emitNull (statement_compiler.go).

### 5.4 Completion / flush barrier (reuses in-band `gdbi.Signal`)
Core processors already forward `Signal` travelers in order (all have `if t.IsSignal() { out <- t }`), and JumpMark relies
on it. Central sends `Flush{n}` after each batch (size/time bound). The node injects `Signal{ID:n}` into every entry
pipeline; when it has emerged from all of them (egress ordering guarantees all earlier results/handoffs were already
sent) it replies `FlushAck{n}`. Central may free `state[rid]` for every request in batch ≤ n after the ack
(results of a request always precede the ack).  **Invariant that every new processor/wrapper must keep: a Signal is never
forwarded before every item that preceded it has been forwarded** (WP4 tests this explicitly, including BoundaryGraph).

Segment termination at central: `input closed && pendingBatches==0 && handoffQueue empty` ⇒ CloseSend on all streams, close out.
Handoffs generated while handling results are enqueued *before* the ack decrements the counter ⇒ no premature termination.

Segment processor receiving a central `Signal` (it sits inside a Jump/Mark loop): drain to quiescence, then forward the
signal downstream.

### 5.5 Fan-out & segmentation rules (planner, WP6)
* Entering traveler routing: vertex current + `out/outE/has*/as/...` → owner(prefix(current.ID)); edge current → owner(prefix(edge.ID));
  vertex current + `in/inE/both/bothE` → owner ∪ `registry.inboundSources(prefix, labels)` (D1; `foreign_probe=true` for non-owners).
* A fan-out step (in/inE/both/bothE where `inboundSources` is non-empty) **terminates the preceding segment** and is the *first*
  step of the next one (so fan-out only ever happens at central entry routing; sources never need to probe other sources).
  If no foreign source declares an inbound edge for that prefix/labels, the step is a normal pushed step. (Trade-off: one
  extra central hop per fan-out step; it keeps node logic simple. Direct node↔node forwarding is a future optimization.)
* Non-pushable step ⇒ segment boundary; planner emits `central step` and continues; the next pushable run starts a new
  segment whose input is the central stream.
* `has` is pushable only if all `tpath` namespaces it references are `_current` or marks created *inside* the same segment
  (use `gdbi/tpath.GetAllNamespaces`).
* Planner runs on the raw statements (after `expandHasAnd`, **before** any backend-specific optimizer). The node applies its own
  optimizers (e.g. `IndexStartOptimize`) to the segment it receives.
* Central runs after-segment steps with `core` processors, using `Extends{StartType, MarksTypes}` state tracking
  (`gdbi.NewPipelineState`), so typing/`StepLoadData` logic is reused. `load_final` comes from `ps.StepLoadData()` of
  the step following the segment.
* Anything the planner can't prove safe → fall back to WP5 baseline for that step (`FederatedGraph` Get*Channel).

### 5.6 Node-side execution (WP4)
* `Start`: validate steps, compute per-cursor element type (`VertexData`/`EdgeData`) from preceding step, compile one
  sub-pipeline per `entry_cursor` using `core.NewCompiler(boundaryGraph, nodeOptimizers...).Compile(steps[k:], &CompileOptions{Extends})`.
* Every entry pipeline = `[Resolve?] → steps[k:] with a Divert processor inserted after each move step`; all feed one egress goroutine
  that batches `TravelerOut`s. Pipelines are long-lived for the stream; travelers are fed by `Batch` messages.
* `BoundaryGraph{inner GraphInterface, prefix}`: for `GetOutChannel/GetInChannel/Get*EdgeChannel/GetVertexChannel`, targets whose ID
  lacks the node prefix are returned as **bare stubs** (`DataElement{ID, Loaded:false}`) instead of being looked up/dropped.
  Generic implementation can compose `inner.Get*EdgeChannel` + `inner.GetVertexChannel`; stubs bypass the vertex fetch but Signals
  must wait for the in-flight inner fetches (send a private Signal through the inner vertex channel and wait for it).
  The table adapter implements this natively.
* `Divert` processor (after each move step k): if `Current.ID` doesn't have the node prefix → send HANDOFF(cursor=k+1) to egress, do not forward; else forward.
* `Resolve` (entry processor): `needs_resolve && !foreign_probe` ⇒ `inner.GetVertexChannel`; drop on missing. Seeds bypass it.
* Delta extraction at egress: node-side travelers start with empty Marks/Path, so `traveler.Marks` and `traveler.Path` *are* the deltas.
  **Risk:** `BaseTraveler.AddCurrent` bootstraps `Path` oddly on first hop; WP2/WP4 must add a differential test (`path()` results federated vs monolithic)
  and fix the seam (central path = `orig.Path ++ delta` without duplicating the entry element).
* Limits on the node: max steps, max in-flight travelers per stream, max stream lifetime; honor ctx cancel (central cancels on `limit` satisfied).

### 5.7 Central ↔ node flow control (deadlock avoidance)
Per `(segment, source)` stream: one sender goroutine and one reader goroutine. Reader **never blocks on dispatch**: handoffs go
to an unbounded (later spill-to-disk, cf. `gripper/queue.go`) queue drained by a router goroutine which feeds other streams' sender queues.
Upstream input is throttled by an in-flight credit window (travelers sent − acked); handoffs are exempt from credits.
A reader may block on `out` (downstream backpressure) — that is safe because no sender depends on it.

### 5.8 Failure semantics
Source unreachable / stream error ⇒ query returns an error (no silent partial results), except D4 drops. Registry health-checks sources;
unavailable-at-start sources are excluded from routing with a warning (as `NewGripServer` does now). `GetInfo` prefix collisions (two sources, same or
overlapping prefix) ⇒ startup error. Cancellation propagates via ctx on every stream.

## 6. Central integration points (existing code that changes)

| File | Change |
|------|--------|
| `server/server.go` | `sources map[string]gripper.GRIPSourceClient` → `*gripper.Registry`; `StartConnection` replaced; `StartDriver` Gripper case → `FederatedGraphDB` |
| `server/metagraphs.go` | remove `__mapping__` handling in `updateGraphMap`; federated graphs come from config / registry |
| `server/api.go` | delete `ListTables`; remove from `gripql.proto` (`TableInfo`, `ListTables`) + regenerate + `gripql/client.go` + python `connection.py::listTables` |
| `server/plugins.go` | keep; `Plugin.client` type follows new `GRIPSourceClient`; after `StartPlugin` call `registry.Add` + re-validate prefixes |
| `config/config.go` | `Sources map[string]string` stays (name→host); `Gripper *gripper.Config` becomes `Federation{Graph string; Sources []string}` (`MappingFile`/`Mapping` removed; also fix redaction at ~l.311 and `GraphToConfig` use ~l.235) |
| `gripper/config.go` | reduced to new config types; `GraphConfig/VertexConfig/EdgeConfig` move to `node/table` as the *node-side* mapping config |
| `cmd/` | new `grip node` subcommand (serve a GraphInterface at a prefix) — optional but needed for WP9; `cmd/erclient` deleted/rewritten |
| `engine/core` | none expected. (Possibly export a helper to build a compiler with `Extends`; `Validate` already honors `Extends`.) |
| `gdbi/traveler.go` | `ApplyDelta`; no change to `Traveler` interface unless needed |
| `docs/website/README` | rewrite gripper docs |

`FederatedGraph` (WP5) `gdbi.GraphInterface` mapping: `GetVertex/GetEdge` = 1-element `V(id)` Traverse at owner;
`VertexLabelScan/GetVertexList` = seeds to label-matching sources; `Get{Out,In,Both…}Channel` = single-step pipelines
(`out(labels)` etc.) with fan-out per D1 and central null aggregation; `ListVertexLabels/ListEdgeLabels/BuildSchema` = union of `SourceInfo.schema`;
mutating methods return "read-only". Index methods: `GetVertexIndexList` aggregates source `indexed_fields`; Add/Delete → not supported.

## 7. Work packages (see SQL todos for IDs/deps)

Each WP lists *acceptance criteria*. All Go code must pass `make lint` and `go test ./...`.

**WP1 proto-design** — rewrite `gripper/gripper.proto` per §3, `make proto`, commit generated code, add `gripper/DESIGN.md`.
DONE. The v1 Go files, `python/`, `gdc-graph/` and `test-graph/` were deleted outright in WP1 (they remain in git history; WP7 may
borrow `Driver`/`BaseRow`/`driver_cache.go` from there). `server/`, `config/` and `cmd/erclient` do not compile until WP3/WP10 adapt them.
Local build tips: see "Build environment" below.

**WP2 wire-and-traveler** — `gripper/wire.go`: `Element<->gdbi.DataElement`, `TravelerIn/Out` builders; `gdbi.BaseTraveler.ApplyDelta(cur DataRef, marks map[string]*DataElement, path []DataElementID) Traveler`.
AC: round-trip unit tests; differential path test (§5.6 risk).

**WP3 registry-and-config** — `gripper/registry.go`: connect to sources (reuse `util/rpc` dial), `GetInfo`, validate prefixes (no equal/overlapping), `Owner(id)` longest-prefix, `InboundSources(prefix, labels)`, `SourcesFor(labels, ids)`; config + `server/` wiring; `ListTables` removal (proto regen for gripql).
AC: unit tests with fake `GRIPSourceClient`; server starts with federation config using stub sources.

**WP4 node-library-core** — `gripper/node/*` per §5.6; `NewServer(graph gdbi.GraphInterface, opts{Name, Prefix})`; `StartServer(port, srv)` helper.
AC: bufconn tests over an in-memory/kvgraph (pebble temp dir) backend: seeds, entry at cursor k, handoff emission, resolve-drop, null, flush ordering (race detector `-race`), ctx cancel, step/limit enforcement.
Depends on WP1, WP2.

**WP5 federated-graph-baseline** — `FederatedGraph`/`FederatedGDB` per §6, using `core.NewCompiler(fed)` (no custom optimizer). Delete `TabularGraph`.
AC: `go test` federated SWAPI (6 in-process node servers on bufconn) matches monolithic kvgraph results for a query corpus (hasLabel, has, out/in/both, *E, *Null, path, select, jump/repeat, limit, count, aggregate).
Depends on WP3, WP4.

**WP6 segment-planner-and-processor** — planner/segment/stream per §5.1–5.7; plugged in as a `QueryOptimizer`/custom compiler of `FederatedGraph.Compiler()`.
AC: differential tests vs WP5 baseline on the same corpus (ordering-insensitive), plus a RPC-count assertion (e.g. `V().hasLabel("Character").out("films").out("planets")` ≤ O(sources×segments) streams, not O(travelers)) and `-race` stress with random handoff patterns; Signal/Jump loop test; `limit` cancels streams promptly.
Depends on WP5.

**WP7 table-adapter** — `gripper/node/table`: Go `Driver` interface (successor of old `Driver`/`BaseRow`; include optional caching from `driver_cache.go`), node-side mapping config (collections→vertex labels, `fieldToField`/`edgeTable`→edge labels; IDs are `<prefix><key>`; edge IDs `from-label-to` as in `sources.go::GenID`), native `GraphInterface` impl with `BoundaryGraph`-equivalent behavior, reference drivers (JSONL/CSV + SQL via `database/sql`/existing-sql schema code).
AC: SWAPI served from tables (reuse `conformance/graphs/swapi.*`) yields identical results to kvgraph-backed node for conformance queries.
Depends on WP4.

**WP8 schema-and-info** — `GetInfo` generation (`node/info.go`), federated `BuildSchema` (union + cross-prefix edges), `ListVertexLabels` etc., schema-based source pruning in planner.
Depends on WP3, WP4 (parallel with WP5).

**WP9 conformance-harness** — `grip node` CLI (`cmd/node`), `test/federated.yml` + script/Makefile target `make test-conformance-federated` that starts 6 node processes (one per SWAPI label, each loaded with its slice; edges stored in the source of the *from* vertex) + central server; run `conformance/run_conformance.py`. Also benchmark in `benchmark/` (federated vs monolithic; parallel clients).
AC: conformance passes at parity with `kvgraph` driver minus documented exclusions; update `.github/skills/grip-conformance`.
Depends on WP6 (WP5 for early smoke).

**WP10 legacy-removal-and-docs** — delete everything listed in §4 "Deleted", python client `listTables`, regenerate gripql protos, rewrite `gripper/README.md` + `website/content/docs/gripper*`, update `AGENTS.md` (architecture + `gripper` skill if useful), Makefile (`start-gripper-test`).
Depends on WP5 (can finish after WP6).

**WP11 hardening** — metrics/log fields (RPC count, handoffs, inflight), limits & timeouts, TLS/auth parity with `util/rpc` + `accounts`, spill-to-disk handoff queue, docs for operators.
Depends on WP6.

## 8. Risks / open items (carry into implementation)

1. **Signal ordering invariant** in BoundaryGraph + backends' batching `Get*Channel` (mongo/psql batch; see `batch[i].IsSignal()`): add generic conformance test for any wrapped backend.
2. **Path seam** (§5.6) and `AddCurrent` subtlety with `ID==""` data-only elements (traveler.go).
3. **Reverse-edge probing cost**: every fan-out step costs a central hop; mitigated by label-pruned `inboundSources`. Owner-existence check for probe sources is skipped (documented inconsistency with D4).
4. **Central memory**: `state[rid]` retention until flush ack; bound via credit window + batch flush timer.
5. **ID/CURIE normalization**: v1 treats IDs as CURIE strings with literal prefix matching; `uri_prefix` expansion is out of scope (can be added to `SourceInfo`).
6. **Ordering**: no ordering guarantee beyond what core gives; `sort` always central.
7. **Auth**: node trusts central; node must still enforce limits (§5.6). mTLS/token story is WP11.
8. `inNull`/`inENull` aggregation correctness under partial failure ⇒ failure = whole-query error (§5.8).
9. Existing `server/server.go` drops unreachable sources silently at startup; federation graphs referencing them must fail clearly.
10. Future: direct node↔node handoff, push `limit`/`count`/`fields`/`render` down, push marks-referencing `has`.

## 9. Non-goals
Backward compat with v1 API/mappings; writes through federated graphs; distributed transactions; Python node SDK.

## 10. Build environment notes (for implementers)

* Regenerate protos for this package: `cd gripper && protoc -I ./ -I ../gripql -I ../googleapis --go_out . --go_opt paths=source_relative --go-grpc_out ./ --go-grpc_opt paths=source_relative gripper.proto`
  (the `proto` Makefile target has the `-I ../gripql` include added for the `gripql.proto` import).
* Known pre-existing failures unrelated to this work: `engine/core` `TestIndexStartOptimize*` (optimizer_test.go) and a panic in `kvgraph/test` `TestDocDelete`;
  `endpoints/cypher` has no `main`; `gdbi` `TestSelectFields`. Record the baseline before judging regressions.

## 11. As built: deviations from the design above

The text above is the original plan. What was implemented differs in these ways; the code and
this section win over the rest of the document.

| Topic | As built |
|-------|----------|
| Flush barrier (§5.4) | **No in-band `Signal` through nodes.** The node runs every `TravelerBatch` through its own short-lived pipeline (grouped by entry cursor, `gripper/node/session.go`), so a batch is complete when its pipeline output closes. `Flush{n}` acks after all batches received before it completed; results and acks share one ordered queue. Central groups travelers into batches that each end with a `Flush`, so an ack identifies the batch (`gripper.BatchStream`). Signals are still used between *central* processors (jump loops): the segment processor quiesces and then forwards the signal. |
| `*Null` steps (D6, §5.3) | **Not pushed.** Nodes reject them (`InvalidArgument`); the planner keeps them central and they run through `FederatedGraph` lookups, which decide "null" from the ack of the batch that contained the lookup. The core engine's null traveler keeps the previous current element (existing quirk), so a node cannot signal "nothing found" reliably. `TravelerOut.null_move` is reserved. |
| Handoff marker (§3) | `TravelerOut.handoff` was added. `cursor == len(steps)` alone is ambiguous when the move is the last step. |
| Segments that don't start with `V` (§5.1) | The central server prefixes a placeholder `V()` (plus `OutE()` when the entering element is an edge) that nodes never execute; travelers enter at the first real step's cursor. The node only accepts seeds at cursor 0 and `V` only as the first step. |
| Start optimization on nodes (§5.6) | `compileSegment` applies `core.IndexStartOptimize` only to the statements before the first move so that moves stay aligned with the original step indexes, and inserts a `divert` processor after every vertex producing move. |
| Traveler type | `gdbi.BaseTraveler.RequestID` carries the request id through the node pipeline; `NewEntryTraveler` / `ApplyDelta` are in `gdbi/traveler.go`. |
| Planner (§5.5) | A statement run is a segment only if it contains a move or starts with `V` (lone filters run centrally). `has` that references marks runs centrally. A fan-out step (`in/inE/both/bothE` with inbound edge links) starts a new segment. |
| Legacy removal (WP1/WP10) | v1 Go code, Python proxies/examples, `ListTables`/`TableInfo`, `GetMapping`/`AddMapping` RPCs, `grip mapping`, `grip er`, `grip list tables` and the `__mapping__` mechanism are deleted. |
| Not done | The native table adapter (WP7); label/field based source pruning beyond vertex labels; surfacing stream failures to the client as query errors (they are logged and end the query early); metrics, TLS/auth parity and handoff spill to disk (WP11); spill/limits on central state. |
| Verification | Go: `gripper/federation_test.go` compares ~30 queries (marks, paths, jump loops, null steps, aggregation...) between a monolithic kvgraph and a six node federation, and asserts the number of streams opened by a pushed-down query. Conformance: `make test-conformance-federated` (83 of 83 tests; `index`, `pivot` and `schema` are excluded because they need writes or the fhir graph). The race detector could not be run in the authoring environment (no cgo); run `go test -race ./gripper/...` where it is available. |
