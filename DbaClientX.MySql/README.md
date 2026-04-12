# DbaClientX.MySql

MySQL provider for DbaClientX (MySqlConnector). Supports non-query, scalar, queries, streaming, transactions, bulk insert.

- Target Frameworks: `net8.0`, `net472`
- NuGet: `DBAClientX.MySql`

## Install

```bash
dotnet add package DBAClientX.MySql
```

## Quick examples

Execute scalar:

```csharp
var my = new DBAClientX.MySql();
var count = my.ExecuteScalar(
    host: "localhost", database: "app", username: "user", password: "p@ss",
    query: "SELECT COUNT(*) FROM users WHERE is_active=1"
);
```

Stream stored procedure results (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in my.ExecuteStoredProcedureStreamAsync(
    host: "localhost", database: "app", username: "user", password: "p@ss",
    procedure: "get_recent_users",
    cancellationToken: ct))
{
    // consume row
}
```

Typed mapped query:

```csharp
var rows = await my.QueryAsListAsync(
    connectionString: "Server=localhost;Database=app;User ID=user;Password=p@ss;SslMode=Required",
    query: "SELECT id, name FROM users ORDER BY id",
    map: row => new UserRow(
        Id: row.GetInt32(row.GetOrdinal("id")),
        Name: row.GetString(row.GetOrdinal("name"))),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

Non-query from a full connection string:

```csharp
await my.ExecuteNonQueryAsync(
    connectionString: "Server=localhost;Database=app;User ID=user;Password=p@ss;SslMode=Required",
    query: "UPDATE users SET last_seen=UTC_TIMESTAMP() WHERE id=@id",
    parameters: new Dictionary<string, object?> { ["@id"] = 1 },
    cancellationToken: ct);
```

Scalar from a full connection string:

```csharp
var count = await my.ExecuteScalarAsync(
    connectionString: "Server=localhost;Database=app;User ID=user;Password=p@ss;SslMode=Required",
    query: "SELECT COUNT(*) FROM users",
    cancellationToken: ct);
```

Stored procedure from a full connection string:

```csharp
var result = await my.ExecuteStoredProcedureAsync(
    connectionString: "Server=localhost;Database=app;User ID=user;Password=p@ss;SslMode=Required",
    procedure: "get_recent_users",
    parameters: new Dictionary<string, object?> { ["p_limit"] = 100 },
    cancellationToken: ct);
```

## See also

- Core mapping + invoker: `DBAClientX.Core`
