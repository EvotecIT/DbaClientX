# DbaClientX.SQLite

SQLite provider for DbaClientX (Microsoft.Data.Sqlite). Supports non-query, scalar, queries, streaming, transactions, bulk insert.

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

## See also

- Core mapping + invoker: `DBAClientX.Core`

