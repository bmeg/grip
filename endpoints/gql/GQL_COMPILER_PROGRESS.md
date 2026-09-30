# GQL Compiler Progress and TODOs

Date: 2026-09-29

This document summarizes the current implementation status of the read-only GQL to GripQL compiler and the most relevant follow-up work.

## Scope Goal

Provide a minimal read-only GQL translator that compiles a constrained subset of queries into GripQL traversal steps.

## Current Status

Implemented in `endpoints/gql/compiler/build.go`.

Supported today:

- `MATCH` with node labels and inline property maps using typed scalar literals
- Linear relationship traversal with `Out`, `In`, and `Both` direction mapping
- Scalar comparison and `IS [NOT] NULL` `WHERE` predicates, with `AND`, `OR`, `NOT`, and parentheses
- `RETURN var`, `RETURN var.field`, and aliased property projections
- Minimal `ORDER BY` on the current traversal variable
- `SKIP` and `LIMIT` integer literals
- Syntax error collection from the generated GQL parser

Intentionally unsupported today:

- `CREATE`, `INSERT`, `SET`, `DELETE`, `REMOVE`
- `WITH`, `UNION`, `OPTIONAL MATCH`, `GROUP BY`, `YIELD`
- Procedure calls and `SELECT`
- Variable-length relationships and quantified paths
- `WHERE XOR`, functions, arithmetic, comparisons to `NULL`, non-literal comparisons, and references to non-current or multiple variables
- Aggregations and complex `RETURN` expressions

## ISO GQL Minimum-Conformance Context

This endpoint implements a deliberately constrained, read-only GQL subset. It is not an ISO/IEC 39075 minimum-conforming implementation, and adding more compiler syntax alone would not make the server conformant.

ISO minimum conformance covers mandatory functionality not gated by optional feature IDs. The required capability areas span session and transaction management, object expressions, query and conditional statements, graph patterns and bindings, predicates, value expressions and functions, lexical elements, and value types. The standard's informative Annex H describes the mandatory functionality; Annex D identifies optional feature groups.

In addition to that mandatory functionality, a minimum-conformance claim requires:

- Support for at least one of automatic graph population (GC00) or graph management (GC04).
- A declared Unicode version of at least 13.0.0; graph character strings use Unicode with `UCS_BASIC` collation.
- At minimum, `STRING`, `BOOL`, signed `INTEGER`, and `FLOAT` property value types.

These requirements cross the parser/compiler boundary into query execution, result/status handling, graph management or population, and possibly session/transaction APIs. Treat the standard as a capability inventory, not as a promise that this endpoint will implement every area. See the [ISO/IEC 39075 catalog entry](https://www.iso.org/standard/76120.html) and [Microsoft's minimum-conformance summary](https://learn.microsoft.com/en-us/fabric/graph/gql-conformance).

## Initial Capability Matrix

This is a first-pass inventory for the compiler, not a server-wide conformance claim. Generated grammar recognition does not mean the compiler or runtime implements the feature. Optional feature IDs are called out separately from minimum-conformance gaps.

| Capability area | ISO classification | Compiler status | GripQL/runtime and test evidence | Next assessment |
|---|---|---|---|---|
| Scalar literals and property types | Required baseline types include `STRING`, `BOOL`, signed `INTEGER`, and `FLOAT`. | Inline maps and comparisons accept strings, integers, floats, booleans, and null. GQL quote doubling, backslash/control escapes, Unicode escapes, and `@` no-escape strings are decoded. | Compiler tests cover literal decoding; endpoint coverage exercises string and boolean predicates. | Establish the server's Unicode version/collation claim separately. |
| Graph matching and bindings | Mandatory graph-pattern capability area. | Supports labeled node patterns and linear chains with directed or undirected relationship traversal. Rejects `OPTIONAL MATCH`, path variables, unsupported directions, quantified relationships, and repeated node variables. | `Out`/`In`/`Both` cover simple traversal; endpoint tests cover multi-hop traversal. Compiler tests verify repeated-variable patterns fail explicitly. | Assess optional and quantified patterns against GripQL operations. Check optional-feature IDs before counting path extensions as required. |
| Predicates and boolean expressions | Comparisons and boolean expressions are mandatory functionality; `XOR` is optional feature GE07. | Supports property-to-scalar comparisons, `IS [NOT] NULL`, and `AND`/`OR`/`NOT` with parentheses and precedence. Requires references to the current variable; rejects `= NULL`, `XOR`, functions, arithmetic, and other operands. | Compiler represents each supported predicate's true and false cases separately, preserving `UNKNOWN` through `AND`/`OR`/`NOT` during filtering. Compiler and Badger endpoint tests cover null predicates and unknown comparisons. | Complete truth-table tests for the supported subset; do not generalize the guarantee to unsupported predicates or expressions. |
| Results, ordering, and paging | `RETURN`, ordering, and paging are in the mandatory query capability area; aggregate functions are part of the mandatory value-expression inventory. | Supports bare/property projections and aliases, current-variable field sorting, `SKIP`, and `LIMIT`. Aggregations and complex return expressions are rejected. | `Render`, `Sort`, `Skip`, and `Limit` are emitted; compiler and endpoint tests cover the basic forms. | Check GripQL aggregate/group execution and result shape before selecting aggregate syntax as the next feature. |
| Query composition and runtime services | Mandatory capability areas also cover query/conditional statements, status objects, sessions, and transactions. | The endpoint currently compiles a narrow single-query read-only form; there is no mapping for the broader statement/runtime areas. | Those concerns are not proven by compiler tests and may live outside the compiler boundary. | Audit server and client APIs separately; classify compiler gaps versus runtime/API work. |
| Graph population or management | At least one of GC00 (automatic graph population) or GC04 (graph management) is additionally required for a minimum-conformance claim. | Not provided as a GQL capability by this read-only compiler. | Repository graph loading/management APIs are outside this compiler inventory and have not been audited for GQL conformance. | Decide whether to expose these through GQL or explicitly retain a non-conformant subset claim. |

Other optional features must be tracked separately. For example, `UNION` is optional feature GQ03; rejection by this compiler is a subset limitation, not by itself a mandatory-feature gap.

## Active Files

- `endpoints/gql/compiler/build.go`
- `endpoints/gql/handle.go`
- `endpoints/gql/test/gql_test.go`
- `endpoints/gql/GQL_TO_GRIPQL.md`
- `endpoints/gql/GRIPQL_GQL_SUPPORT_PLAN.md`

## Recent Implementation

- Inline property-map values preserve string, integer, float, boolean, and null types.
- GQL string literals decode their defined escape forms; repeated node-variable paths are rejected rather than silently rebound.
- Boolean `WHERE` comparisons support `AND`, `OR`, `NOT`, and parentheses with GQL precedence; paired true/false filters preserve `UNKNOWN` for these supported forms. `XOR` remains unsupported.
- `WHERE var.field IS NULL` and `IS NOT NULL` compile through GripQL nil equality; comparisons to `NULL` are rejected.
- Compiler tests assert predicate trees, and the endpoint test exercises boolean filtering against a seeded graph.

## Refined Development Plan

1. **Complete the inventory.** The matrix above is the initial compiler view. Add evidence links/test names and audit the server/runtime rows before making any whole-product conformance statement.
2. **Continue semantic correctness.** Complete truth-table coverage for the supported predicate subset and verify null/string behavior consistently across backends. Repeated-variable paths remain explicitly rejected until GripQL can preserve their equality semantics. Broader predicate forms remain unsupported until their truth semantics can be preserved.
3. **Expand based on backend evidence.** Assess `OPTIONAL MATCH`, aggregates, and grouped results one at a time. Document the translation and backend limitations before adding table-driven compiler and endpoint tests.
4. **Keep scope explicit.** If the goal becomes ISO minimum conformance, separately plan graph population/management, status objects, Unicode behavior, and session/transaction APIs. Otherwise, keep the endpoint read-only and document it as a supported subset, not a conformant implementation.

For the staged GripQL protocol/runtime plan and first suggested implementation task, see `GRIPQL_GQL_SUPPORT_PLAN.md`.