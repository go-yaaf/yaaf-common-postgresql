# yaaf-common-postgresql

[![Build](https://github.com/go-yaaf/yaaf-common-postgresql/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-postgresql/actions/workflows/build.yml)

Postgresql object database implementation of `yaaf-common` IDatabase interface

## About
This library is the Postgresql concrete implementation of the ORM layer defined in the `IDatabase` interface in `yaaf-common` library.
This implementation refers to the object database concepts only, the underlying database includes table per domain model entity,
each table has only two fields: `id` (of type string) and `data` (of type jsonb).
Domain model entities are stored as Json documents (which are indexed by json keys) hence the database is used like a document storage (similar to Elasticsearch, MongoDB, Couchbase and more)

This library is built around the [Pure Go Postgres driver for database/sql](https://github.com/lib/pq)

#### Adding dependency

```bash
$ go get -v -t github.com/go-yaaf/yaaf-common-postgresql
```

