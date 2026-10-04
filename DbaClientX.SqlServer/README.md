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

## Estimated query plans

`ExplainQueryPlanAsync` captures SQL Server's estimated plan for one SELECT, INSERT, UPDATE, DELETE or MERGE
statement without running that statement. It uses a separate non-pooled connection outside the client's active
transaction and ambient enlistment. The login needs statement permissions and SHOWPLAN permission in each
referenced database.

```csharp
using DBAClientX;
using DBAClientX.QueryPlans;
using System.Data;

using var client = new SqlServer();
var plan = await client.ExplainQueryPlanAsync(connectionString,
    "SELECT Payload FROM dbo.Events WHERE Id = @id",
    new Dictionary<string, SqlServerQueryPlanParameter> {
        ["@id"] = new(SqlDbType.Int)
    });

foreach (var step in plan.Steps) {
    Console.WriteLine($"{step.Detail}: {step.Schema}.{step.Table}, output={step.Estimates?.OutputRows}, read={step.Estimates?.RowsRead}");
}
```

Parameter types create unassigned local variables. These are **generic estimates**, recorded as
`plan.Provenance.ParameterMode == TypedVariables`; parameter values are never interpolated or bound. Names must
match the SQL spelling exactly, with ASCII letters, digits and underscores. Character/binary declarations require
an explicit size, such as `new(SqlDbType.NVarChar, size: 200)`; use `-1` for MAX. Decimal and temporal declarations
accept precision/scale facets. Table-valued, UDT and legacy LOB declarations are unsupported.

Native `Estimates` keep fractional output rows, estimated rows read and table cardinality separate. Missing
estimates remain null. `ScanOperations` lists scan access methods: TOP/MAX can stop after one row, so the operation
label alone does not establish a full traversal. `QueryPlanAssert`, `QueryPlanRules` and `FullScans` use SQLite
semantics and reject native SQL Server plans. SQL Server's `SELECT WITHOUT QUERY` classification is retained with
an empty operator list for constant-only statements.

`SqlServerQueryPlanParser.Parse(sql, showPlanXml)` reads an existing estimated SHOWPLAN document with the same
model. Capture and parsing accept at most 16,777,216 document characters and 4096 operators. Actual runtime plans,
procedural batches and multiple statement plans are unsupported. Capture uses the client's `CommandTimeout`;
cancellation still attempts SHOWPLAN cleanup with a separate five-second deadline. Custom connection factories
must return a closed connection with pooling and enlistment disabled.

## Database workload diagnostics

Read Query Store configuration and a bounded rowstore table-index observation for one database:

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

`GetQueryStoreStateAsync` and `GetIndexUsageAsync` also collect these sections individually. Query Store reports actual/desired state, capture mode, storage and the complete read-only reason bitmask. Collection does not enable Query Store, alter capture policy, update statistics or return captured query text. Index usage retains primary-key/uniqueness roles, nullable read/write counters and index statistics properties. The default continuous-monitoring scope remains `Baseline`. `All` retains its published value and original sections; use `All | Workload` to collect them together.

The index limit is between 1 and 10,000. `IsTruncated` reports additional visible indexes; catalog permissions can hide objects even when it is false. This collector includes rowstore table indexes and excludes indexed views, heaps, columnstore, hypothetical and memory-optimized indexes. Missing usage rows or statistics properties remain null. A zero or absent counter does not prove that an index is unused. Counters can reset on instance restart, database detach/shutdown and index changes, so `ServerStartTime` gives restart context rather than a complete observation window. Server-reported timestamps retain their local clock values with an unspecified `DateTime.Kind`; `CollectedUtc` is the collector's UTC completion time.

Query Store needs SQL Server 2016 or later and the applicable database-state/performance-state permission. A disabled database reports `OFF`; `master` and `tempdb` report `unsupported`. Missing configuration metadata reports `metadata-unavailable` rather than being interpreted as `OFF`. Index/statistics collection needs SQL Server 2014 or later and the applicable server-state/performance-state permission; statistics/catalog visibility also depends on the caller's grants. A failed optional section remains null and appears in `Errors`, including permission-denied and unsupported classifications. Cancellation propagates to the caller. Collection makes no automatic index-removal recommendation.

The PowerShell monitoring command forwards the same scope and row bound:

```powershell
Get-DbaXSqlServerMonitoring -Server sql01 -Database App -Scope Workload -MaximumIndexUsageRows 1000
```

## Backup and restore qualification

Backup paths and relocation paths are evaluated on the SQL Server host under its service account. Supply existing directories that the service can read and write. The SQL login needs backup permission on the source, permission to create the restored database, and permission to run CHECKDB. These operations use an owned connection to `master` outside ambient transactions.

```csharp
using var sql = new SqlServer();
var backup = await sql.BackupDatabaseCopyOnlyToDiskAsync(
    connectionString, "App", serverBackupDirectory, cancellationToken: ct);

// Retain this identity with the recovery record, separately from the backup file.
var identity = backup.Header.Identity;
var files = await sql.ReadDiskBackupFileListAsync(
    connectionString, backup.ServerBackupPath, cancellationToken: ct);
string targetName = "App_Recovery_" + Guid.NewGuid().ToString("N");
var destinations = new Dictionary<string, string>();
for (int index = 0; index < files.Count; index++)
{
    var file = files[index];
    string directory = file.FileType switch
    {
        "D" => serverDataDirectory,
        "L" => serverLogDirectory,
        _ => throw new NotSupportedException("Assign an explicit destination for this file type.")
    };
    destinations.Add(file.LogicalName, directory.TrimEnd('\\', '/') + "/"
        + targetName + "_" + index + (file.FileType == "L" ? ".ldf" : ".mdf"));
}

var plan = await sql.PrepareRestoreAsNewAsync(connectionString,
    backup.ServerBackupPath, targetName, identity, destinations, cancellationToken: ct);
// Review plan.Header, plan.Files, plan.FileDestinations and plan.RequiredFileBytes.
await sql.RestoreDatabaseAsNewAsync(connectionString, plan, cancellationToken: ct);
var integrity = await sql.CheckDatabaseIntegrityAsync(
    connectionString, targetName, cancellationToken: ct);
if (!integrity.Succeeded)
    throw new InvalidOperationException("CHECKDB reported integrity issues.");
```

Preparation reads the backup identity and file metadata, refuses an existing target or assigned paths, and runs `RESTORE VERIFYONLY` with the complete `MOVE` mapping and checksums. Its immutable result records the allocated file sizes; that sum excludes growth, CHECKDB and other temporary space. Preparation does not reserve names, files or free space. Execution revalidates the plan and coordinates library callers with a target application lock. It never uses `REPLACE`. Keep other restore tools away from the target and protect the backup media from modification; the pinned media identity is not an authenticity signature.

Verification proves readability and checksums. A restore followed by full CHECKDB provides separate database integrity evidence. Application readiness still needs application checks. `PrepareRestoreAsNewAsync` and `RestoreDatabaseAsNewAsync` accept one dedicated full backup. No recovery command is replayed, including when command retries are enabled.

Canceling a running backup or restore propagates `OperationCanceledException` with the caller token when SQL Server acknowledges cancellation. Native abort and termination messages accompanying that acknowledgement are accepted; unrelated failures remain errors. Cancellation does not roll back a restore or remove its database and server files. Inspect the target state and arrange explicit cleanup before reusing its name or paths.

CHECKDB returns a typed result with a total issue count and at most `MaxIssues` retained records (default 1,000). Message text is excluded unless `IncludeDiagnosticMessages` is true. `PhysicalOnly = true` narrows the check and is recorded in the result; full checks are the default. CHECKDB can consume substantial CPU, I/O and temporary space. Execution failures throw the library's redacted query exception, and cancellation preserves the caller token.

The restored database remains available for application checks. The caller owns its eventual deletion and the backup file's retention policy. A cancelled or failed restore may leave a database in `RESTORING` state; confirm the target name and relocated files belong to the attempted restore before deleting it. A failed backup can leave an incomplete file in the supplied directory. The library does not delete caller-named databases or server files automatically.

### Full, differential and log chains

`BackupToDedicatedDiskAsync` creates a dedicated full, differential or log backup. Its required `copyOnly` argument makes the source policy explicit: a conventional full establishes a differential base, and a conventional log backup can permit log truncation. Use `BackupDatabaseCopyOnlyToDiskAsync` for the existing copy-only full workflow. Every generated backup uses checksums and is verified before its identity is returned.

Prepare an explicitly ordered chain from retained backup results. Each disk file must contain one backup set and one media family:

```csharp
// full, differential and log are previously verified SqlServerDiskBackupResult values.
var sources = new[] {
    new SqlServerRestoreChainSource(full.ServerBackupPath, full.Header.Identity),
    new SqlServerRestoreChainSource(differential.ServerBackupPath, differential.Header.Identity),
    new SqlServerRestoreChainSource(log.ServerBackupPath, log.Header.Identity)
};
// Supply one new service-visible relocation path per logical file, as in the example above.
var chain = await sql.PrepareRestoreChainAsNewAsync(
    connectionString, sources, targetName, destinations, cancellationToken: ct);
Console.WriteLine($"Recorded file allocation: {chain.RequiredFileBytes} bytes");
await sql.RestoreChainAsNewAsync(connectionString, chain, cancellationToken: ct);
var checkedDatabase = await sql.CheckDatabaseIntegrityAsync(
    connectionString, targetName, cancellationToken: ct);
if (!checkedDatabase.Succeeded)
    throw new InvalidOperationException("CHECKDB reported integrity issues.");
```

The full backup comes first, followed by at most one differential and then contiguous log backups. The differential must identify its conventional full base; a copy-only full cannot serve as that base. Native log ranges may overlap the previous ending LSN, but must cover and advance it. Sequence numbers retain SQL Server's full precision. Preparation rejects changed database families, recovery forks, snapshot backups, incomplete metadata and changed file identities, including a file recreated under the same logical name.

The immutable plan pins every backup identity and relocation. Execution revalidates the chain, holds one target application lock on an owned non-pooled connection, applies each step with `NORECOVERY`, and issues a separate final `RECOVERY`. It refuses an existing target and never uses `REPLACE`. It does not discover or reorder backups, cross recovery forks, resume a partial restore, restore to a point in time, or cut over an application.

`RequiredFiles` records each file's maximum allocation across the supplied headers; `RequiredFileBytes` sums those allocations. Native relocation and capacity preflight checks the full backup. Budget later growth, CHECKDB and temporary space separately; the plan reserves neither capacity nor files. Cancellation can retain a `RESTORING` database and server files for explicit inspection and cleanup.

To run the opt-in local recovery contracts, set `DBACLIENTX_SQL_BACKUP_TEST_CONNECTION`, `DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY`, and `DBACLIENTX_SQL_BACKUP_TEST_RESTORE_DIRECTORY`, then select `Category=LiveSqlRecovery`. Supply directories writable by both the test process and SQL Server service; default instance directories can have service-only permissions. Tests create uniquely named databases and remove their own databases and files.

## See also

- Core mapping + invoker: `DBAClientX.Core`
- Other providers: PostgreSql, MySql, SQLite, Oracle
