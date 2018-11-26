---
title: SQL

menu:
  main:
    parent: Databases
    weight: 4
---

# Connect to an existing SQL database

GRIP supports modeling an existing SQL database as a graph. GRIP has been tested against [PostgreSQL][psql], but should  work with
[MySQL][sql] (4.1+) and [MariaDB][maria].

Since GRIP uses Go's `database/sql` package, we could (in thoery) support any SQL databases listed on:
https://github.com/golang/go/wiki/SQLDrivers. Open an [issue](https://github.com/bmeg/grip/issues/new) if you
would like to request support for your favorite SQL database.

## Configuration Notes

* `DataSourceName` is a driver-specific data source name, usually consisting of at least a database name and connection information. Here are links for
to documentation for this field for each supported driver:

  * PostgreSQL - https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters
  * MySQL, Mariadb - https://github.com/go-sql-driver/mysql/#dsn-data-source-name

* `Driver` should be one of: `postgres` or `mysql` (for MySQL or MariaDB).

* `Graphs` is a list of graphs you want to define using the existing tables in the database. For each entry:

  * `Graph` is the name of the graph you want to define
  * `Vertices` is a list of entries, each of which binds a table to a vertex `label` and defines which field in the table to use as the `gid`.
  The remaining columns in the table are treated as the `data` associated with the vertex.
  * `Edges` is a list of entries. Edge entries may be associate an edge `label` with a table, but they are not required to.
  See below for examples of both types of edge definitions.

## Example

Given the following example database: https://github.com/bmeg/grip/blob/master/test/resources/postgres_smtest_data.dump

Load this dump file into your own postgres instance by running:

```bash
createdb --host localhost --port 15432 -U postgres smtest
psql --host localhost --port 15432 -U postgres smtest < postgres_smtest_data.dump
```

GRIP Configuration:

```yaml
Database: existing-sql

SQL:
  DataSourceName: "host=localhost port=15432 user=postgres dbname=smtest sslmode=disable"
  Driver: postgres

  Graphs:
    - Graph: test-graph
      Vertices:
        - Table: users
          Label: users
          GidField: id

        - Table: products
          Label: products
          GidField: id

        - Table: purchases
          Label: purchases
          GidField: id

      Edges:
        - Table: purchase_items
          Label: purchasedProducts
          GidField: id
          From:
            SourceField: purchase_id
            DestTable: purchases
            DestField: id
          To:
            SourceField: product_id
            DestTable: products
            DestField: id

        - Table: ""
          Label: userPurchases          
          GidField: ""
          From:
            SourceField: ""
            DestTable: users
            DestField: id
          To:
            SourceField: ""
            DestTable: purchases
            DestField: user_id
```

[psql]: https://www.postgresql.org/
[sql]: https://www.mysql.com/
[maria]: https://mariadb.org/
