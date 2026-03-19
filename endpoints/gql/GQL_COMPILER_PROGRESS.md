# GQL Compiler Progress and TODOs

Date: 2026-03-19

This document summarizes the current implementation status of the read-only GQL to GripQL compiler and the most relevant follow-up work.

## Scope Goal

Provide a minimal read-only GQL translator that compiles a constrained subset of queries into GripQL traversal steps.

## Current Status

Implemented in `gql/compiler/build.go`.

Supported today:

- `MATCH` with node labels and inline property maps
- Linear relationship traversal with `Out`, `In`, and `Both` direction mapping
- Minimal `WHERE` predicates of the form `var.field <op> literal`
- `RETURN var`, `RETURN var.field`, and aliased property projections
- Minimal `ORDER BY` on the current traversal variable
- `SKIP` and `LIMIT` integer literals
- Syntax error collection from the generated GQL parser

Intentionally unsupported today:

- `CREATE`, `INSERT`, `SET`, `DELETE`, `REMOVE`
- `WITH`, `UNION`, `OPTIONAL MATCH`, `GROUP BY`, `YIELD`
- Procedure calls and `SELECT`
- Variable-length relationships and quantified paths
- Complex `WHERE` expressions
- Aggregations and complex `RETURN` expressions

## Active Files

- `gql/compiler/build.go`
- `gql/handle.go`
- `gql/test/gql_test.go`
- `gql/GQL_TO_GRIPQL.md`

## Recent Cleanup

- Renamed active compiler symbols and error messages to refer to GQL consistently.
- Consolidated endpoint integration on the active GQL handler path.
- Removed the unused legacy parser implementation tree.
- Kept the working compiler behavior unchanged for the supported subset.

## Highest-Value Next Steps

1. Add structured compiler error categories instead of plain strings.
2. Expand `WHERE` support to boolean composition while keeping explicit scope rules.
3. Add more whitespace, casing, and malformed-query coverage in `gql/test/gql_test.go`.
4. Decide whether to support simple aggregation pipelines or reject them permanently.