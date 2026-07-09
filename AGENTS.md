# AGENTS.md

Guidance for AI coding assistants working in this repository. Keep changes consistent with the conventions and **security invariants** below.

## What this library is

`yaaf-common-postgresql` is the PostgreSQL implementation of the `IDatabase` /
`IDatastore` ORM interfaces defined in [`yaaf-common`](https://github.com/go-yaaf/yaaf-common).
It is a **document store built on top of PostgreSQL JSONB**, not a relational mapper.

- One table per entity type. Every table has exactly two columns:
  - `id` — `character varying` primary key
  - `data` — `jsonb` (the entity serialized as JSON)
- Entity fields are queried through JSONB path expressions (`data->>'field'`),
  optionally cast to a SQL type and/or backed by a BTREE expression index.
- The design goal is to avoid schema migrations when domain models change.

Module path: `github.com/go-yaaf/yaaf-common-postgresql` · Go **1.25.8** · driver **`jackc/pgx/v5`**.

> Note: `README.md` and `llms.txt` contain some stale details (they mention `lib/pq`
> and Go 1.23). Trust the code and `go.mod` over those documents.

## Layout

```
postgresql/
  postgres_database.go               # IDatabase/IDatastore: connection, CRUD, bulk, DDL, SetField(s)
  postgres_database_query.go         # IQuery: Find/Count/Aggregation/Histogram/GroupBy/Delete/SetFields
  postgres_database_query_helper.go  # SQL string building: buildCriteria, buildFilter, getCastField, isSafeFieldName
  postgres_database_notifications.go # LISTEN/NOTIFY pub-sub (Subscribe + trigger DDL)
test/                                # Docker-based integration tests (require a live Postgres)
  model/                             # sample entities used by tests
```

There is **no `internal/` package** — everything public lives under `postgresql/`.

## Build / test / verify

```bash
go build ./...      # must stay green
go vet ./...        # must stay green
gofmt -l .          # should print nothing (tabs, standard Go formatting)
```

Tests under `test/` are **integration tests that need a running PostgreSQL** and are
skipped in CI (they call `skipCI(t)` / check the `CI` env var, or spin up a Docker
container via `utils.DockerUtils()`). Do not assume they run in a sandbox.

- Provide a DB connection to tests via the `TEST_DB_URI` environment variable, e.g.
  `export TEST_DB_URI="postgres://user:pwd@localhost:5432/test_db"`. Tests skip when unset.
- **Never hardcode real credentials or host IPs** in test files (this repo had a
  leaked credential incident — keep secrets in env vars only).

## Security invariants — READ BEFORE EDITING QUERY CODE

This library builds SQL with `fmt.Sprintf`. The rules below are load-bearing; a
change that violates them reintroduces SQL injection.

1. **Values are always bind parameters (`$1`, `$2`, …), never interpolated.**
   Use `pgx` placeholders and pass values through `args`. Do not build a value
   into a SQL string, even inside a quoted literal. For JSONB values, encode with
   `json.Marshal` and bind as `$N::jsonb` (see `SetField`, `Contains`).

2. **Identifiers (field / column / sort / table names) cannot be bind parameters,
   so they MUST be validated with `isSafeFieldName(...)`** before interpolation.
   `isSafeFieldName` (in `postgres_database_query_helper.go`) allows only
   `[A-Za-z0-9_.[]]`. Any new code path that puts a field/table name into SQL must
   gate it:
   - `getCastField` is the central choke point for filter/sort/order fields — it
     already collapses unsafe names to a non-matching literal `'__invalid_field__'`.
   - Aggregation/group/histogram helpers validate `field`/`timeField`/`dim` and
     return an error on unsafe input.
   - `SetField`, `SetFields`, `BulkSetFields`, `buildFilterArrayLike`, and
     `buildSubQueryFilter` each validate field names explicitly.

3. **`ExecuteSQL` / `ExecuteQuery` are raw pass-throughs.** They are the caller's
   responsibility; still pass their `args` as bind parameters, never format user
   data into the `sql` string.

4. **Connection security:** `convertConnectionString` honors `sslmode` from the URI
   and defaults to `require`. Do not reintroduce a hardcoded `sslmode=disable`.
   Do not echo raw connection URIs in errors/logs — use `uri.Redacted()`.

5. **LISTEN/NOTIFY:** channel names are validated with `isSafeChannel` before being
   interpolated into `LISTEN`. Keep that guard for any identifier used in
   notification DDL.

## Conventions

- **Errors:** return them; do not panic in library code (tests may panic). Wrap with
  `fmt.Errorf("...: %w", err)` where a cause is useful. Watch for the common bug of
  checking the wrong error variable (`if …; err != nil` vs the freshly returned `er`).
- **Entities** implement `yaaf-common/entity.Entity`, embed `entity.BaseEntity`, define
  `TABLE()`, and are constructed via an `EntityFactory` (`func() Entity`). Query APIs
  take the factory, not an instance.
- **Sharding / multi-tenant:** table names may contain a `{key}` placeholder resolved by
  `tableName(table, keys...)`. Methods accept trailing `keys ...string` for this.
- **Change publishing:** mutations call `publishChange(...)` which no-ops unless a
  message bus was injected via `NewPostgresDatabaseWithMessageBus`.
- Keep new code in the same file-region style (`// region … ` / `// endregion` banners)
  and match surrounding naming.

## Gotchas

- The query builder mutates receiver state (e.g. `Find` appends the range filter into
  `allFilters`, `FindSingle` sets `limit = 1`). Treat a built query as single-use.
- `getCastField` only casts fields present in the reflected entity field map
  (`entityFieldsToTypesMap`); unknown-but-safe names fall through as native column
  references (intended, for native-indexed columns).
- Several `IDatastore` index methods (`CreateIndex`, `ListIndices`, `DropIndex`, …) are
  intentionally unimplemented and return `"not implemented"`.
- `pgconn v1.x` (legacy) is still pulled in alongside `pgx/v5`; prefer `pgx/v5` APIs.

## When making changes

- After editing, run `go build ./...` and `go vet ./...`.
- If you touch query/SQL construction, re-check the five security invariants above.
- Do not commit or push unless explicitly asked. If asked, branch off `main` first.
