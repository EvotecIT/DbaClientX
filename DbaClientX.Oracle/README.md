# DbaClientX.Oracle

Oracle provider for DbaClientX (Oracle.ManagedDataAccess). Supports non-query, scalar, queries, streaming, transactions, bulk insert.

- Target Frameworks: `net8.0`, `net472`
- NuGet: `DBAClientX.Oracle`

## Install

```bash
dotnet add package DBAClientX.Oracle
```

## Quick examples

Execute non-query:

```csharp
var ora = new DBAClientX.Oracle();
ora.ExecuteNonQuery(
    host: "oraclesrv", serviceName: "orclpdb1", username: "user", password: "p@ss",
    query: "UPDATE users SET last_login = SYSTIMESTAMP WHERE id = :id",
    parameters: new Dictionary<string,object?> { [":id"] = 1 }
);
```

Stream query (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in ora.QueryStreamAsync(
    host: "oraclesrv", serviceName: "orclpdb1", username: "user", password: "p@ss",
    query: "SELECT id, name FROM users ORDER BY id",
    cancellationToken: ct))
{
    // consume row
}
```

Typed mapped query:

```csharp
var rows = await ora.QueryAsListAsync(
    connectionString: "Data Source=oraclesrv/orclpdb1;User Id=user;Password=p@ss",
    query: "SELECT id, name FROM users ORDER BY id",
    map: row => new UserRow(
        Id: row.GetInt32(row.GetOrdinal("ID")),
        Name: row.GetString(row.GetOrdinal("NAME"))),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

Non-query from a full connection string:

```csharp
await ora.ExecuteNonQueryAsync(
    connectionString: "Data Source=oraclesrv/orclpdb1;User Id=user;Password=p@ss",
    query: "UPDATE users SET last_login = SYSTIMESTAMP WHERE id = :id",
    parameters: new Dictionary<string, object?> { [":id"] = 1 },
    cancellationToken: ct);
```

Scalar from a full connection string:

```csharp
var count = await ora.ExecuteScalarAsync(
    connectionString: "Data Source=oraclesrv/orclpdb1;User Id=user;Password=p@ss",
    query: "SELECT COUNT(*) FROM users",
    cancellationToken: ct);
```

Stored procedure from a full connection string:

```csharp
var result = await ora.ExecuteStoredProcedureAsync(
    connectionString: "Data Source=oraclesrv/orclpdb1;User Id=user;Password=p@ss",
    procedure: "GET_RECENT_USERS",
    parameters: new Dictionary<string, object?> { [":limit"] = 100 },
    cancellationToken: ct);
```

## See also

- Core mapping + invoker: `DBAClientX.Core`
