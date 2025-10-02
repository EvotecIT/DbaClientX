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

## See also

- Core mapping + invoker: `DBAClientX.Core`

