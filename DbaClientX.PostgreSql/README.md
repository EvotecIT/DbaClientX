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

Non-query from a full connection string:

```csharp
await pg.ExecuteNonQueryAsync(
    connectionString: "Host=localhost;Database=app;Username=user;Password=p@ss;SSL Mode=Require",
    query: "UPDATE public.users SET last_seen=NOW() WHERE id=@id",
    parameters: new Dictionary<string, object?> { ["@id"] = 1 },
    cancellationToken: ct);
```

Scalar from a full connection string:

```csharp
var count = await pg.ExecuteScalarAsync(
    connectionString: "Host=localhost;Database=app;Username=user;Password=p@ss;SSL Mode=Require",
    query: "SELECT COUNT(*) FROM public.users",
    cancellationToken: ct);
```

Stored procedure from a full connection string:

```csharp
var result = await pg.ExecuteStoredProcedureAsync(
    connectionString: "Host=localhost;Database=app;Username=user;Password=p@ss;SSL Mode=Require",
    procedure: "public.refresh_stats",
    parameters: new Dictionary<string, object?> { ["@days"] = 7 },
    cancellationToken: ct);
```

## Estimated query plans

`ExplainQueryPlan` and `ExplainQueryPlanAsync` capture one native PostgreSQL estimated plan. Named values are bound through Npgsql:

```csharp
var plan = await pg.ExplainQueryPlanAsync(
    connectionString: "Host=localhost;Database=app;Username=user;Password=p@ss;SSL Mode=Require",
    query: "SELECT id FROM public.users WHERE id=@id",
    parameters: new Dictionary<string, object?> { ["id"] = 42 },
    cancellationToken: ct);

foreach (var step in plan.Steps)
{
    Console.WriteLine($"{step.Detail}: {step.Schema}.{step.Table}, output={step.Estimates?.OutputRows}");
}
```

Use `parameterTypes` with existing `NpgsqlDbType` values when a null or array needs an explicit type. Bound values record the parameter context; they do not guarantee a particular server plan-cache policy. Positional `$1` parameters and scripts are unsupported.

Capture uses `EXPLAIN (ANALYZE FALSE, VERBOSE TRUE, COSTS TRUE, FORMAT JSON)` on a separate connection and read-only transaction. It leaves active client and ambient transactions alone, ignores command replay settings, honors `CommandTimeout` and caller cancellation, and attempts rollback and disposal after failure. It requires `standard_conforming_strings=on` and TLS. Planning can still acquire locks and invoke planner-time functions; read-only protection prevents local database writes, without sandboxing external effects of custom functions.

`PostgreSqlQueryPlanParser.Parse(sql, explainJson)` imports the same estimated JSON format. Native names retain their spelling and case. Operator IDs are assigned in preorder, with root parent `-1`. `Plan Rows` maps to output rows and `Total Cost` to subtree cost. Rows read, table cardinality and database identity remain unknown when the document does not supply them; parallel estimates are not multiplied. `ScanOperations` reports access methods. `FullScans` and `QueryPlanAssert` remain SQLite-specific and reject PostgreSQL plans.

The reader rejects actual execution counters, duplicate properties and multiple statement plans. Limits are 16,777,216 JSON characters, 128 nesting levels and 4096 operators. Capture accepts one SELECT, WITH, INSERT, UPDATE, DELETE or MERGE statement of at most 1,048,576 characters.

## See also

- Core mapping + invoker: `DBAClientX.Core`
