# DbaClientX.PostgreSql

PostgreSQL provider for DbaClientX. Simple API over Npgsql with streaming, retries, and transactions.

- Target Frameworks: `net472`, `net8.0`, `net10.0`
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

Typed mapped query:

```csharp
int idOrdinal = -1;
int nameOrdinal = -1;
var rows = await pg.QueryAsListAsync(
    connectionString: "Host=localhost;Database=app;Username=user;Password=p@ss;SSL Mode=Require",
    query: "SELECT id, name FROM public.users ORDER BY id",
    initialize: row => {
        idOrdinal = row.GetOrdinal("id");
        nameOrdinal = row.GetOrdinal("name");
    },
    map: row => new UserRow(
        Id: row.GetInt32(idOrdinal),
        Name: row.IsDBNull(nameOrdinal) ? null : row.GetString(nameOrdinal)),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

## See also

- Core mapping + invoker: `DBAClientX.Core`
