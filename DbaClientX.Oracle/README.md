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

## Verified table copies and checkpoints


`OracleTableCopyAdapter` preserves the physical case and punctuation of source result columns. Use explicitly delimited key names, such as `new[] { "\"Id\"" }`, for mixed-case Oracle columns. Direct `BulkInsertAsync` accepts the same explicit double-quoted destination mappings; ordinary unquoted mappings retain Oracle's name folding.

For checkpointed copies, create destination tables with `SEGMENT CREATION IMMEDIATE` or allocate their storage explicitly before copying. Oracle's serializable checkpoint writes require existing storage; preflight rejects deferred destinations before writing rows. Static partitioned tables require allocated storage for every leaf partition or subpartition. Interval and automatic list partitioning are unsupported for checkpoint destinations, including automatic subpartitions: native partition creation cannot be rolled back with a probe's rows. The adapter creates its own permanent `DbaX_TableCopyCheckpoints` table with immediate storage and materializes a pre-existing empty deferred checkpoint table. It does not allocate storage for user destination tables or retry failed pages. Transactional string writes retain `NCHAR` and `NVARCHAR2` national-character bindings.

Checkpoint cancellation retains the caller token and committed pages. Set `Resume = true` with the same checkpoint identifier for an explicit resume; the engine verifies the committed prefix before continuing. `ReadConsistency = DbaTableCopyReadConsistency.Snapshot` holds an Oracle serializable source transaction for a consistent copy.

## See also

- Core mapping + invoker: `DBAClientX.Core`

## Estimated query plans

`ExplainQueryPlan` and `ExplainQueryPlanAsync` capture one SELECT or WITH statement without executing the explained query. Supply an explicit `tcps://` Easy Connect data source or a descriptor whose addresses all use `PROTOCOL=TCPS`, with the wallet/trust settings required by the server. Capture enforces server DN matching and preserves the supplied connection settings; unresolved TNS aliases and plaintext fallback addresses are unsupported.

```csharp
var plan = await ora.ExplainQueryPlanAsync(
    connectionString: tcpsConnectionString,
    query: "SELECT e.id FROM events e WHERE e.id = :id",
    parameters: new Dictionary<string, object?> { ["id"] = 42 },
    parameterTypes: new Dictionary<string, OracleDbType> { ["id"] = OracleDbType.Int32 },
    cancellationToken: ct);

foreach (var step in plan.Steps)
    Console.WriteLine($"{step.Id} <- {step.ParentId}: {step.Detail}; output={step.Estimates?.OutputRows}");
```

The operation owns a non-pooled, non-enlisted connection and uses Oracle's built-in `SYS.PLAN_TABLE$` only after verifying that it is temporary. It creates and drops no plan tables, leaves active client/ambient transactions separate, and rolls back its session-owned rows before returning. Ordinary capture needs `CREATE SESSION`, access to the explained objects, and Oracle's standard temporary plan-table grants. Writes, scripts, local PL/SQL functions and quoted/positional bind names are unsupported. Named binds start with a letter and may contain letters, digits, `_`, `$` and `#`; provider types and typed nulls use the existing binding rules. Native commands are never replayed, even when the client enables retries.

`CommandTimeout` or the native command timeout controls capture. Rollback uses an independent five-second cancellation deadline/native command timeout; cleanup failures prevent successful delivery. Caller cancellation also prevents delivery if it arrives during cleanup. Parsing and optimization may acquire metadata locks or invoke optimizer functions; their external effects cannot be rolled back. Oracle's estimated plan can differ from a cached execution plan, particularly with bind variables, and some implicit date-bind conversions are unsupported. See [Oracle's execution-plan documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/generating-and-displaying-execution-plans.html).

For offline import, call `OracleQueryPlanParser.Parse(sql, planRows)`. The `DataTable` requires `ID`, `PARENT_ID` and `OPERATION`; optional native columns are `PLAN_ID`, `OPTIONS`, `OBJECT_OWNER`, `OBJECT_NAME`, `OBJECT_TYPE`, `OBJECT_ALIAS`, `CARDINALITY` and `COST`. Use CLR numeric values for numeric columns, converting any provider-specific wrappers first. The parser retains operator IDs, parent IDs, physical names, native alias text and nullable output/cardinality and cost estimates. Index names are not inferred to be table names. Rows read, table cardinality and database remain unknown; `BoundValues` records binding rather than guaranteeing optimizer bind peeking. Native `FullScans` and `QueryPlanAssert` are unsupported.

Import and capture reject mixed plans, duplicate IDs, missing parents, cycles, depth above 128, more than 4096 operators or text fields above 4096 characters. SQL capture is limited to 1,048,576 characters.
