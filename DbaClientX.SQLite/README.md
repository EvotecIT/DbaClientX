# DbaClientX.SQLite

SQLite provider for DbaClientX (Microsoft.Data.Sqlite). Supports non-query, scalar, queries, streaming, transactions, bulk insert.
Also includes SQLite maintenance helpers for online database backup, WAL checkpointing, `PRAGMA optimize`, and graceful shutdown preparation.

- Target Frameworks: `net472` (win-x64), `netstandard2.1`, `net8.0`, `net10.0`
- NuGet: `DBAClientX.SQLite`

## Install

```bash
dotnet add package DBAClientX.SQLite
```

## Quick examples

Query table:

```csharp
var sq = new DBAClientX.SQLite();
var result = sq.Query(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id"
);
```

Stream query (netstandard2.1+/net8.0):

```csharp
await foreach (DataRow row in sq.QueryStreamAsync(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id",
    cancellationToken: ct))
{
    // consume row
}
```

Typed mapped query:

```csharp
var rows = await sq.QueryAsListAsync(
    database: "app.db",
    query: "SELECT Id, Name FROM Users ORDER BY Id",
    map: row => new UserRow(
        Id: row.GetInt32(row.GetOrdinal("Id")),
        Name: row.GetString(row.GetOrdinal("Name"))),
    cancellationToken: ct);
```

For larger result sets, use `QueryStreamAsync<T>` with the same mapper shape to avoid buffering all rows.

Execute against a full connection string without losing provider options:

```csharp
var connectionString = new SqliteConnectionStringBuilder
{
    DataSource = "app.db",
    Mode = SqliteOpenMode.ReadWrite,
    Cache = SqliteCacheMode.Shared,
    Pooling = false,
    DefaultTimeout = 15
}.ConnectionString;

await sq.ExecuteNonQueryWithConnectionStringAsync(
    connectionString,
    "UPDATE Users SET Name = @name WHERE Id = @id",
    new Dictionary<string, object?> { ["@name"] = "Alice", ["@id"] = 1 },
    cancellationToken: ct);
```

`SQLiteGeneric.GenericExecutors.ExecuteSqlAsync` accepts either a path or a full connection string. Full connection strings preserve mode, cache, pooling, timeout, password, foreign-key, trigger, and VFS settings. File paths containing `=` remain valid paths.

Writing many rows in one transaction with a prepared command (the statement is parsed once; each call rebinds values by position):

```csharp
var sq = new DBAClientX.SQLite();
using var session = sq.OpenSession("app.db");
session.RunInTransaction(tx =>
{
    using var insert = tx.PrepareInsert("Users", "Id", "Name", "Status");
    foreach (var user in users)
    {
        insert.ExecuteNonQuery(user.Id, user.Name, user.Status); // null -> NULL, enum -> integer
    }
});

// Any statement with named parameters, e.g. an insert that returns the new key:
using var addRun = session.Prepare("INSERT INTO Runs (RunId) VALUES ($runId); SELECT last_insert_rowid();", "$runId");
long runKey = (long)addRun.ExecuteScalar("run-42")!;
```

Graceful shutdown maintenance:

```csharp
var sq = new DBAClientX.SQLite();
await sq.PrepareForShutdownAsync(
    database: "app.db",
    options: new SqliteShutdownMaintenanceOptions
    {
        CheckpointMode = SqliteCheckpointMode.Truncate,
        OptimizeAfterCheckpoint = true
    });
```

Backup before maintenance:

```csharp
var sq = new DBAClientX.SQLite();
sq.BackupDatabase("app.db", "backups/app.db");
```

Backup of a database that may be written meanwhile (a service's live store), with progress and cancellation:

```csharp
var sq = new DBAClientX.SQLite();
SqliteBackupResult result = await sq.BackupDatabaseAsync(
    "app.db",
    "backups/app.db",
    SqliteBackupMethod.Auto,
    new SqliteBackupOptions { OverwriteDestination = true, PagesPerStep = 4096 },
    progress: new Progress<SqliteBackupProgress>(p => Console.WriteLine($"{p.PercentComplete:F0}%")),
    cancellationToken);
// result.Method is Snapshot for a WAL database and Incremental otherwise.
```

`BackupDatabase` and `BackupDatabaseAsync` with `SqliteBackupMethod.Auto` copy a WAL database as a snapshot
(`BackupDatabaseSnapshotAsync`) and any other database step-wise (`BackupDatabaseIncrementalAsync`), reading the journal
mode from the database file header. The snapshot copy holds the database as it was when the backup started and
completes while other connections write. While it runs, the WAL keeps what is written meanwhile, and a `FULL`,
`RESTART` or `TRUNCATE` checkpoint waits for it (for the checkpointing connection's busy timeout, with writers waiting
behind the checkpoint). The step-wise copy releases the source between steps, but every change by another connection
restarts it: it suits a database in rollback-journal mode (where a snapshot is a shared lock that keeps writers from
committing until the copy ends), writes that pause long enough for a whole copy, or a database that is not being
written. Pass `SqliteBackupMethod.Snapshot` or `Incremental` to choose. A rollback-journal database that is written
continuously needs WAL mode to be backed up online.

PowerShell: `Invoke-DbaXSQLiteMaintenance -Database app.db -Action Backup -Destination backups\app.db` uses the same
choice (`-BackupMethod Auto|Snapshot|Incremental`), shows progress and stops the copy on Ctrl+C.

Unicode case-insensitive search and sort (SQLite's own `lower()`, `LIKE` and `NOCASE` fold ASCII only):

```csharp
var sq = new DBAClientX.SQLite { ConfigureConnection = SQLiteUnicodeText.Register };
var rows = await sq.QueryReadOnlyAsListAsync(
    "app.db",
    "SELECT Name FROM Hosts WHERE dbx_lower(Name) = dbx_lower(@name) ORDER BY Name COLLATE DBX_NOCASE",
    reader => reader.GetString(0),
    new Dictionary<string, object?> { ["@name"] = "ŻÓŁW" });
```

Indexed substring search for large, rarely written tables (FTS5 trigram index kept current by triggers):

```csharp
await sq.CreateTrigramIndexAsync("app.db", "HostsSearch", "Hosts", "Id", new[] { "Name", "Owner" });
var query = new Query().Select("Id", "Name").From("Hosts");
if (SQLiteTrigramSearch.CanMatch(text))
    query.WhereInRaw(SqlIdentifier.Quote(SqlDialect.SQLite, "Id"), SQLiteTrigramSearch.MatchingKeys("HostsSearch", text));
else
    query.WhereContains("Name", text, TextFolding.Invariant); // needs SQLiteUnicodeText registered, as above
```

Read-only streaming, with the running statement interrupted when the token is canceled:

```csharp
await foreach (var row in sq.QueryReadOnlyStreamAsync("app.db", "SELECT Id, Name FROM Hosts", DbaRecordMapper.Values(), cancellationToken: ct))
{
    // one row at a time
}
```

## See also

- Core mapping + invoker: `DBAClientX.Core`
