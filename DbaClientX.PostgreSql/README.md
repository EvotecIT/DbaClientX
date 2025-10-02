# DbaClientX.PostgreSql

PostgreSQL provider for DbaClientX. Simple API over Npgsql with streaming, retries, and transactions.

- Target Frameworks: `net8.0`, `net472`
- NuGet: `DBAClientX.PostgreSql`

## Install

```bash
dotnet add package DBAClientX.PostgreSql
```

## Quick examples

Execute stored procedure:

```csharp
var pg = new DBAClientX.PostgreSql();
var result = pg.ExecuteStoredProcedure(
    host: "localhost", database: "app", username: "user", password: "p@ss",
    procedure: "refresh_stats",
    parameters: new Dictionary<string,object?> { ["@days"] = 7 }
);
```

Stream query (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in pg.QueryStreamAsync(
    host: "localhost", database: "app", username: "user", password: "p@ss",
    query: "SELECT id, name FROM public.users ORDER BY id",
    cancellationToken: ct))
{
    // consume row
}
```

## See also

- Core mapping + invoker: `DBAClientX.Core`

