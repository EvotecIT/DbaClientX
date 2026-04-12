# DbaClientX.SQLite

SQLite provider for DbaClientX (Microsoft.Data.Sqlite). Supports non-query, scalar, queries, streaming, transactions, bulk insert.
Also includes SQLite maintenance helpers for WAL checkpointing, `PRAGMA optimize`, and graceful shutdown preparation.

- Target Frameworks: `net8.0`, `net472`
- NuGet: `DBAClientX.SQLite`

## Install

```bash
dotnet add package DBAClientX.SQLite
```

## Quick examples

Query table:

```csharp
var sq = new DBAClientX.SQLite();
var result = sq.Query(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id"
);
```

Stream query (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in sq.QueryStreamAsync(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id",
    cancellationToken: ct))
{
    // consume row
}
```

Typed mapped query:

```csharp
var rows = await sq.QueryAsListAsync(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id",
    map: row => new UserRow(
        Id: row.GetInt32(row.GetOrdinal("Id")),
        Name: row.GetString(row.GetOrdinal("Name"))),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

Graceful shutdown maintenance:

```csharp
var sq = new DBAClientX.SQLite();
await sq.PrepareForShutdownAsync(
    database: "app.db",
    options: new SqliteShutdownMaintenanceOptions
    {
        CheckpointMode = SqliteCheckpointMode.Truncate,
        OptimizeAfterCheckpoint = true
    });
```

## See also

- Core mapping + invoker: `DBAClientX.Core`
