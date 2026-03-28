# DbaClientX.SqlServer

SQL Server provider for DbaClientX. Thin, fast ADO.NET wrapper with streaming, retries, and transactions.

- Target Frameworks: `net8.0`, `net472`
- NuGet: `DBAClientX.SqlServer`

## Install

```bash
dotnet add package DBAClientX.SqlServer
```

## Quick examples

Execute non-query:

```csharp
var cli = new DBAClientX.SqlServer();
cli.ExecuteNonQuery(
    serverOrInstance: ".", database: "App", integratedSecurity: true,
    query: "UPDATE Users SET LastLogin=SYSUTCDATETIME() WHERE Id=@Id",
    parameters: new Dictionary<string,object?> { ["@Id"] = 1 }
);
```

Query + stream rows (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in cli.QueryStreamAsync(
    serverOrInstance: ".", database: "App", integratedSecurity: true,
    query: "SELECT TOP 100 Id, Name FROM dbo.Users ORDER BY Id",
    cancellationToken: ct))
{
    // use row["Id"], row["Name"]
}
```

Transactions:

```csharp
cli.RunInTransaction(
    serverOrInstance: ".",
    database: "App",
    integratedSecurity: true,
    operation: tx => tx.ExecuteNonQuery(".", "App", true, "DELETE FROM Logs WHERE Level='Debug'", useTransaction: true)
);
```

## See also

- Core mapping + invoker: `DBAClientX.Core`
- Other providers: PostgreSql, MySql, SQLite, Oracle
