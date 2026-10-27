
# GQL Endpoint

A read-only GQL (ISO/IEC 39075) endpoint that compiles a constrained query subset into GripQL
and executes it against a named graph. The server exposes `POST /gql/{graphName}` with body
`{"query": "MATCH ..."}`; rows are streamed back as JSON lines. A CLI wrapper is available:
`grip gql <graph> "<GQL query>"`.

## Documentation

| Document | Contents |
|---|---|
| `GQL_TO_GRIPQL.md` | The supported GQL subset and its GripQL translation, with example mappings. |
| `COMPILER_ARCHITECTURE.md` | How `compiler/build.go` is organized, known quirks, and the checklist for extending it. |
| `GQL_COMPILER_PROGRESS.md` | Implementation status, ISO conformance context, and prioritized TODOs. |

## Building / Regenerating the Parser

The parser in `parser/` is generated from the [opengql/grammar](https://github.com/opengql/grammar)
GQL grammar via ANTLR. Regenerate only when the grammar or generator version changes:

```
curl -O https://www.antlr.org/download/antlr-4.13.2-complete.jar
./generate.sh
```

## Tests

```
go test ./endpoints/gql/...
```

Compiler behavior is covered by table-driven tests in `test/gql_test.go`; each case pairs a GQL
query with the exact expected `*gripql.Query` (statement-for-statement via `proto.Equal`) or an
expected compile error.
