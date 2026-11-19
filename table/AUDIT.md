# Table GraphInterface Audit

## Read operations

| GraphInterface method | Table adapter behavior |
|---|---|
| `Compiler` | Uses the core engine compiler. |
| `GetTimestamp` | Returns `table`; exact timestamps require a backend capability. |
| `GetVertex` / `GetEdge` | Uses the adapter's in-memory indexes; `load=false` returns an ID/label/endpoint stub. |
| `VertexLabelScan` | Streams deterministic vertex IDs for a label. |
| `ListVertexLabels` / `ListEdgeLabels` | Derived from mapping and loaded rows. |
| `GetVertexList` | Streams sorted, mapped vertices. |
| `GetVertexChannel` | Point lookup with signal forwarding and cancellation. |
| `GetOutChannel` / `GetInChannel` | In-memory adjacency with optional null output and edge-label filtering. |
| `GetOutEdgeChannel` / `GetInEdgeChannel` | In-memory edge lookup with optional null output and edge-label filtering. |
| `BuildSchema` | Available through `GraphDB.BuildSchema` and core schema scanning. |

## Optional and mutation operations

| Operation | Status |
|---|---|
| `AddVertex` / `AddEdge` | Supported only when the table implements `WriteTableDriver` and advertises `Writable=true`. |
| `DelVertex` / `DelEdge` | Supported only through the write capability. Missing rows are currently no-ops for deletes. |
| `BulkAdd` / `BulkDel` | Delegates element operations; bulk write interfaces are not required. |
| Vertex indexes | Unsupported. Indexed fields remain backend metadata and do not mutate graph indexes. |
| Edge index mutation | No separate edge index API exists. |
| Collection timestamps | Not currently exposed. |

## Remaining optimization opportunities

- Avoid materializing the entire table into memory for live backends; add a direct graph adapter mode.
- Push indexed primary-ID and field lookups into `TableDriver.LookupByID` and `LookupByField`.
- Add batched lookup support and use `BatchTableDriver.LookupByIDs` for channels.
- Add backend-native label scans instead of mapping all vertices.
- Add an edge map keyed by label and endpoints to reduce edge fanout.
- Add cursor/limit propagation to graph construction and large collection scans.
- Track graph mutations and timestamps in write-capable adapters.
- Return explicit duplicate-edge semantics rather than allowing map key overwrite.
- Add direct edge data for in-memory rows and avoid re-cloning maps in hot channels.
- Add plugin ABI metadata beyond constructor signature, including Go/platform/CGO compatibility.
