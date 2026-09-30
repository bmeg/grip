# GQL to GripQL Mapping

This document describes how the GQL compiler in this repository translates queries into GripQL.

The current implementation is intentionally minimal and read-only. Unsupported features return explicit compile errors.

## Translator Entry Point

- Compiler package: `endpoints/gql/compiler/build.go`
- Parse function: `RunParser(gql string) (*gripql.Query, error)`

## GripQL Syntax Used by the Compiler

The compiler currently emits these GripQL steps:

- `V()`
- `HasLabel(...)`
- `Has(gripql.Eq(...))` for inline node map properties
- `Has(gripql.Eq/Neq/Gt/Gte/Lt/Lte(...))` for supported WHERE predicates
- `Has(gripql.And/Or/Not(...))` for supported boolean WHERE composition
- `Out(...)`, `In(...)`, and `Both(...)` for relationship traversal
- `As(...)`
- `SameAs(...)`
- `Sort(...)`
- `Render(...)`
- `Skip(...)`
- `Limit(...)`

## Supported GQL Subset

### MATCH with one node pattern

GQL:

```gql
MATCH (n:Person {name: 'Bob'})
RETURN n
```

GripQL:

```go
gripql.NewQuery().
  V().
  HasLabel("Person").
  Has(gripql.Eq("name", "Bob")).
  As("n").
  Render("$n")
```

Notes:

- One node pattern is supported.
- Node labels are mapped to `HasLabel`.
- Node inline map properties are mapped to `Has(gripql.Eq(...))`.
- Inline map values preserve string, integer, float, boolean, and null scalar types.
- Character strings decode GQL quote doubling, backslash/control escapes, and `\\uXXXX`/`\\UXXXXXX` Unicode escapes. The `@` no-escape prefix preserves backslashes.
- A single projection item in `RETURN` is rendered.

### MATCH with linear relationship traversal

GQL:

```gql
MATCH (n)-[:FRIEND]->(friend)
RETURN friend
```

GripQL:

```go
gripql.NewQuery().
  V().
  As("n").
  Out("FRIEND").
  As("friend").
  Render("$friend")
```

Direction mapping, applied per hop in the chain:

- `-[:TYPE]->` maps to `Out("TYPE")`
- `<-[:TYPE]-` maps to `In("TYPE")`
- `-[:TYPE]-` maps to `Both("TYPE")`

Linear multi-hop chains are supported by applying the same mapping repeatedly.

### Repeated node variables

GQL:

```gql
MATCH (n)-[:FRIEND]->(next)-[:FRIEND]->(n)
RETURN n
```

GripQL constrains the final node to the original binding:

```go
gripql.NewQuery().
  V().
  As("n").
  Out("FRIEND").
  As("next").
  Out("FRIEND").
  SameAs("n").
  Render("$n")
```

`SameAs` retains only travelers whose current vertex ID matches the vertex bound to the named variable. Repeated relationship variables are rejected until the compiler supports binding edge elements.

### MATCH with minimal WHERE predicate

GQL:

```gql
MATCH (n:Person)
WHERE n.name = 'Bob'
RETURN n
```

GripQL:

```go
gripql.NewQuery().
  V().
  HasLabel("Person").
  As("n").
  Has(gripql.Eq("name", "Bob")).
  Render("$n")
```

Supported WHERE forms:

- `var.field = literal`
- `var.field != literal` and `var.field <> literal`
- `var.field > literal`
- `var.field >= literal`
- `var.field < literal`
- `var.field <= literal`
- `var.field IS NULL`
- `var.field IS NOT NULL`
- Comparisons composed with `AND`, `OR`, `NOT`, and parentheses

Boolean composition follows GQL precedence: `AND` binds more tightly than `OR`.
The compiler maps these expressions to GripQL `And`, `Or`, and `Not` expression trees.
`IS NULL` maps to `Eq(field, nil)` and `IS NOT NULL` to `Not(Eq(field, nil))`; the engine and grid filter treat a missing property as null for these predicates.
 Ordinary comparisons to `NULL` (such as `field = NULL`) are rejected. For supported comparisons and null predicates, the compiler builds separate true/false filters so `AND`, `OR`, and `NOT` preserve `UNKNOWN` when filtering. This guarantee does not extend to unsupported predicate forms or expression operands.

WHERE scope rule, current implementation:

- The `var` in `WHERE` must be the current traversal variable, the last node in the path.

### RETURN projection forms

Supported RETURN forms:

- `RETURN var`
- `RETURN var.field`
- `RETURN var.field AS alias`
- Multi-item combinations of the above

Example:

```gql
MATCH (n:Person {name: 'Bob'})
RETURN n.name AS personName
```

```go
gripql.NewQuery().
  V().
  HasLabel("Person").
  Has(gripql.Eq("name", "Bob")).
  As("n").
  Render(map[string]any{"personName": "$n.name"})
```

### Pagination clauses

GQL:

```gql
MATCH (n:Person)
RETURN n
SKIP 5
LIMIT 10
```

GripQL:

```go
gripql.NewQuery().
  V().
  HasLabel("Person").
  As("n").
  Render("$n").
  Skip(5).
  Limit(10)
```

Notes:

- `SKIP` and `LIMIT` currently accept only integer literals.

### ORDER BY clause

GQL:

```gql
MATCH (n:Person)
RETURN n
ORDER BY n.name DESC
```

GripQL:

```go
gripql.NewQuery().
  V().
  HasLabel("Person").
  As("n").
  Sort([]*gripql.SortField{{Field: "name", Descending: true}}).
  Render("$n")
```

Notes:

- Supported form is `ORDER BY var.field [ASC|DESC]`.
- Multiple sort keys are supported, comma-separated.
- The `var` in `ORDER BY` must be the current traversal variable.

## Unsupported GQL Features, Current

The compiler currently rejects queries containing any of the following:

- Variable-length relationships, for example `*1..2`
- `WHERE XOR`, function calls, arithmetic, comparisons to `NULL`, and non-literal comparisons
- Full three-valued boolean semantics for `UNKNOWN`
- `WHERE` expressions that reference multiple variables
- Repeated edge variables, because the current GQL mapping does not bind edge elements
- `WHERE` on a non-current traversal variable
- `WITH`
- `SET`
- `DELETE`
- `REMOVE`
- `MERGE`
- `UNWIND`
- `UNION`
- Complex `RETURN` expressions, for example functions like `count(n)`, arithmetic, or nested expressions
- `CREATE`, write operations

## Error Behavior

Unsupported features return an error from `RunParser` and do not emit partial fallback queries.

## Developer Notes for Extending

When extending the translator:

- Keep `gql/compiler/build.go` as the translation source of truth.
- Add table-driven tests in `gql/test/gql_test.go` for every new supported syntax shape.
- Keep unsupported features explicit until fully implemented.
- Add new mapping examples to this document and verify they compile through `RunParser`.