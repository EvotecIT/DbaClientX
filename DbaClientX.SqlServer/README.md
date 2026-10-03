# DbaClientX.SqlServer

SQL Server provider for DbaClientX. Thin, fast ADO.NET wrapper with streaming, retries, and transactions.

- Target Frameworks: `net472`, `net8.0`, `net10.0`
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

Typed mapped query:

```csharp
var rows = await cli.QueryAsListAsync(
    connectionString: "Server=.;Database=App;Integrated Security=True;Trust Server Certificate=True",
    query: "SELECT TOP 100 Id, Name FROM dbo.Users ORDER BY Id",
    map: row => new UserRow(
        Id: row.GetInt32(row.GetOrdinal("Id")),
        Name: row.GetString(row.GetOrdinal("Name"))),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

Use the owned `DbDataReader` surface when another API consumes a reader directly:

```csharp
await using var reader = await cli.QueryReaderAsync(
    connectionString: "Server=.;Database=App;Integrated Security=True;Encrypt=True",
    query: "SELECT Id, Name FROM dbo.Users ORDER BY Id",
    cancellationToken: ct);

while (await reader.ReadAsync(ct))
{
    var id = reader.GetInt32(0);
    var name = reader.GetString(1);
}
```

Disposing the reader also disposes its command and any connection opened for it. Output parameters are copied back after the provider reader closes. `DisposeAsync` uses provider asynchronous cleanup when available.

Non-query from a full connection string:

```csharp
await cli.ExecuteNonQueryAsync(
    connectionString: "Server=.;Database=App;Integrated Security=True;Encrypt=True",
    query: "UPDATE dbo.Users SET LastLogin=SYSUTCDATETIME() WHERE Id=@Id",
    parameters: new Dictionary<string, object?> { ["@Id"] = 1 },
    cancellationToken: ct);
```

Scalar from a full connection string:

```csharp
var count = await cli.ExecuteScalarAsync(
    connectionString: "Server=.;Database=App;Integrated Security=True;Encrypt=True",
    query: "SELECT COUNT(*) FROM dbo.Users",
    cancellationToken: ct);
```

Stored procedure from a full connection string:

```csharp
var result = await cli.ExecuteStoredProcedureAsync(
    connectionString: "Server=.;Database=App;Integrated Security=True;Encrypt=True",
    procedure: "dbo.GetRecentUsers",
    parameters: new Dictionary<string, object?> { ["@Limit"] = 100 },
    cancellationToken: ct);
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

## Database workload diagnostics

Read Query Store configuration and a bounded rowstore-index observation for one database:

```csharp
var snapshot = await cli.GetMonitoringSnapshotAsync(
    new DBAClientX.SqlServerMonitoring.SqlServerMonitoringTarget
    {
        ServerOrInstance = "sql01",
        Database = "App",
        IntegratedSecurity = true
    },
    new DBAClientX.SqlServerMonitoring.SqlServerMonitoringOptions
    {
        Scope = DBAClientX.SqlServerMonitoring.SqlServerMonitoringScope.Workload,
        MaximumIndexUsageRows = 500
    },
    cancellationToken: ct);

if (snapshot.QueryStore is { } store)
    Console.WriteLine($"Query Store: {store.ActualState}, requested {store.DesiredState}");

if (snapshot.IndexUsage is { } usage)
    Console.WriteLine($"Visible indexes: {usage.Indexes.Count}, truncated: {usage.IsTruncated}");

foreach (var error in snapshot.Errors)
    Console.WriteLine(error);
```

`GetQueryStoreStateAsync` and `GetIndexUsageAsync` also collect these sections individually. Query Store reports actual/desired state, capture mode, storage and the complete read-only reason bitmask. Collection does not enable Query Store, alter capture policy, update statistics or return captured query text. Index usage retains primary-key/uniqueness roles, nullable read/write counters and index statistics properties. The default continuous-monitoring scope remains `Baseline`; `All` includes the workload sections.

The index limit is between 1 and 10,000. `IsTruncated` reports additional visible indexes; catalog permissions can hide objects even when it is false. This collector includes rowstore indexes and excludes heaps, columnstore, hypothetical and memory-optimized indexes. Missing usage rows or statistics properties remain null. A zero or absent counter does not prove that an index is unused. Counters can reset on instance restart, database detach/shutdown and index changes, so `ServerStartTime` gives restart context rather than a complete observation window. Server-reported timestamps retain their local clock values with an unspecified `DateTime.Kind`; `CollectedUtc` is the collector's UTC completion time.

Query Store needs SQL Server 2016 or later and the applicable database-state/performance-state permission. Index/statistics collection needs SQL Server 2014 or later and the applicable server-state/performance-state permission; statistics/catalog visibility also depends on the caller's grants. A failed optional section remains null and appears in `Errors`, including permission-denied and unsupported classifications. Cancellation propagates to the caller. Collection makes no automatic index-removal recommendation.

## See also

- Core mapping + invoker: `DBAClientX.Core`
- Other providers: PostgreSql, MySql, SQLite, Oracle
