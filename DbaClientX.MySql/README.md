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

## Estimated query plans

Capture a native SELECT/WITH plan without executing the explained statement:

```csharp
using DBAClientX.QueryPlans;
using MySqlConnector;

var plan = await my.ExplainQueryPlanAsync(connectionString,
    "SELECT u.id FROM users AS u WHERE u.id=@id",
    new Dictionary<string, object?> { ["id"] = 42 },
    new Dictionary<string, MySqlDbType> { ["id"] = MySqlDbType.Int32 },
    cancellationToken: ct);

foreach (var step in plan.Steps)
    Console.WriteLine($"{step.Detail}: native name={step.Table}, rows examined={step.Estimates?.RowsRead}");
```

The synchronous method has the same contract. Named `@name` and `?name` parameters use the existing MySqlConnector binding, including typed nulls. Scripts, executable comments, positional parameters and EXPLAIN/ANALYZE controls are rejected. Capture uses a separate TLS connection and read-only transaction, with pooling and automatic enlistment disabled. It preserves active client transactions, never replays native commands, and checks cancellation after cleanup before delivering a plan. Native transaction startup/disposal defaults to five seconds; rollback has its own five-second deadline and native cancellation uses two seconds. The client's `CommandTimeout` controls capture; when unset, the supplied connection's native command timeout applies.

MySQL and MariaDB reject EXPLAIN of writes in read-only transactions. Capture therefore supports SELECT/WITH permitted by that transaction, and does not offer a writable mode. Session `NO_BACKSLASH_ESCAPES` and `ANSI_QUOTES` are rejected before EXPLAIN because they change lexical interpretation. Native EXPLAIN may acquire metadata locks and invoke optimizer functions. A read-only transaction constrains persistent table writes, not external function effects. Use appropriate permissions for the statements being planned.

Offline import explicitly selects the native format:

```csharp
var imported = MySqlQueryPlanParser.Parse(sql, explainJson,
    MySqlQueryPlanFormat.MySqlJsonV1); // Or MariaDbJson
```

The reader supports MySQL JSON v1 and MariaDB estimated JSON, including nested loops, materialized subqueries, unions, ordering and window groups. Other JSON versions, runtime ANALYZE counters, duplicate properties and unknown wrappers containing operators are rejected. Documents are limited to 16,777,216 characters, 128 JSON nesting levels and 4096 operators. IDs are assigned by the reader in preorder, not taken from native `select_id`.

Native `table_name` may be an alias or a synthesized name such as `<derived2>`; the reader preserves it without guessing the base table, schema, database or separate alias from SQL. MySQL `rows_examined_per_scan` and MariaDB `rows` populate `RowsRead`. `OutputRows` and `TableRows` remain unknown. MySQL `rows_produced_per_join` and `prefix_cost` describe a cumulative join prefix; they remain in `Detail` rather than becoming per-operator output or subtree estimates. Query-block `query_cost` populates `SubtreeCost` in native units. Scan/search labels describe access methods and cannot establish full traversal; native `FullScans` and `QueryPlanAssert` are unsupported.

.NET Framework uses System.Text.Json for native JSON parsing; .NET 8 and .NET 10 use their in-box implementation. Existing provider and plan constructors are unchanged.

## See also

- Core mapping + invoker: `DBAClientX.Core`
