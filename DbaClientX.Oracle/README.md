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

## See also

- Core mapping + invoker: `DBAClientX.Core`

