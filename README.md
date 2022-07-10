# yaaf-common-postgresql

[![Build](https://github.com/mottyc/yaaf-common-postgresql/actions/workflows/build.yml/badge.svg)](https://github.com/mottyc/yaaf-common-postgresql/actions/workflows/build.yml)

PostgreSQL implementation of `yaaf-common` IDatabase interface

## About
This library is the PostgreSQL concrete implementation of the ORM layer defined in the `IDatabase` interface in `yaaf-common` library.

This library is built around the [Pure Go Postgres driver for database/sql](https://github.com/lib/pq)

#### Adding dependency

```bash
$ go get -v -t github.com/mottyc/yaaf-common-postgresql ./...
```

