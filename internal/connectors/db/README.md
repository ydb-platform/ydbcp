# Metadata connector

Application code uses `DBConnector` with domain entities, typed filters and
`Changes`. SDK sessions, transaction modes, YQL, schema names and row/path
encoding stay inside this package. Query builders and codecs are protected by
Go's nested `internal` boundary. Customer-database export/import belongs to the
separate `client` connector.

- Get methods return `ErrNotFound`, inspectable with `errors.Is`.
- Lists return empty results when nothing matches. Filters are ANDed; values
  within a status/type filter are ORed. Time bounds are inclusive.
- Lists default to descending creation time. Child operations are ordered by
  ascending creation time. Ordering of ties is unspecified.
- Database-name masks retain substring LIKE semantics, including percent and
  underscore wildcards.
- A nil page is unbounded. Public API page defaults, decimal tokens and protobuf
  conversions are handled in `server/services/listoptions`.
- Expiry uses database time; the mock uses its injectable clock.
- `Apply` commits the entire batch in one transaction. It does not make earlier
  reads or external export/S3 requests part of that transaction.
- Creates preserve supplied IDs and use upserts; updates preserve the mutable
  field rules documented on `Changes`. Empty batches do nothing.
- Operation creation metrics are emitted after successful persistence, once per
  Apply call, outside SDK retries.
- `MockDBConnector` copies inputs and outputs, evaluates filters and summaries,
  and applies updates atomically. Use `WithApplyError` to test failed writes.

`NewYdbConnector` and `Close` are for connection ownership in main.
`RunMigrations` and `MigrationConnection` encapsulate schema maintenance.

Run connector and application contract tests with:

```sh
go test -race ./internal/connectors/db/... ./internal/server/services/... ./internal/handlers/... ./internal/watchers/...
```

Connector unit tests exercise SDK calls through a private session seam. Existing
Docker integration programs (`cmd/integration/orm`, `list_entities`, and
`migrate`) use the public connector to exercise persistence against YDB.
