# DbaClientX.Core

Core building blocks for DbaClientX: retryable execution pipeline, parameter mapping for POCOs/dictionaries, SQL query builder/compiler, and light utilities.

- Target Frameworks: `net472`, `netstandard2.1`, `net8.0`, `net10.0`
- NuGet: `DBAClientX.Core`

## Install

```bash
 dotnet add package DBAClientX.Core
```

## Quick start (POCO → parameters → execute)

```csharp
using DBAClientX.Invoker;
using DBAClientX.Mapping;

var items = new [] { new { Id = 1, Name = "Alice" } };
var map   = new Dictionary<string,string> { ["Id"] = "@Id", ["Name"] = "@Name" };

await DbInvoker.ExecuteSqlAsync(
    providerAlias: "sqlserver",          // sqlserver|mssql|postgresql|postgres|pgsql|mysql|sqlite|oracle
    connectionString: "Server=.;Database=App;Trusted_Connection=True;",
    sql: "INSERT INTO Users(Id,Name) VALUES(@Id,@Name)",
    items: items,
    map: map,
    options: new DbParameterMapperOptions { DateTimeOffsetAsUtcDateTime = true }
);
```

Provider aliases, canonical names, and generic executor type names come from `DbaConnectionFactory.SupportedProviders`. Use `DbaConnectionFactory.TryGetProvider` when a host needs to normalize user input without maintaining its own alias switch.

`DbInvoker` enumerates input lazily when no batch size is set. Parallel execution uses a fixed worker set, so the number of queued operations does not grow with the input sequence; `ParallelDegree` is capped by `DbExecutionOptions.MaximumParallelDegree`.

## Query builder (safe identifiers and explicit raw SQL)

```csharp
using DBAClientX.QueryBuilder;

var sql = QueryBuilder
    .Select("Id", "Name")
    .From("dbo.Users")
    .Where("IsActive", true)
    .OrderBy("Name")
    .Compile(SqlDialect.SqlServer);
```

Identifier methods quote every identifier; spaces and parentheses no longer switch the compiler into raw-SQL mode. Use an explicit raw method only for trusted expressions:

```csharp
var aggregate = new Query()
    .Select("DepartmentId")
    .SelectRaw("COUNT(*)")
    .From("Employees")
    .GroupBy("DepartmentId")
    .HavingRaw("COUNT(*)", ">", 5)
    .OrderByRaw("COUNT(*) DESC");
```

For aliased joins, prefer the identifier overload so tables, aliases, and both sides of the condition are quoted:

```csharp
var joined = new Query()
    .Select("u.Id", "o.Total")
    .From("Users", "u")
    .Join("Orders", "o", "u.Id", "=", "o.UserId");
```

`SelectRaw`, `FromRaw`, `JoinRaw`, `WhereRaw`, `WhereContainsRaw`, `GroupByRaw`, `HavingRaw`, and `OrderByRaw` emit caller-authored SQL. Never pass user input to these methods. The legacy two-string join overloads remain available for migration but are obsolete because they treat both arguments as raw SQL. Comparison operators are limited to the supported safe operator set, and `Limit`, `Offset`, and `Top` reject negative values.

To put a mapped or user-supplied name into such a fragment, quote it with `SqlIdentifier.Quote(dialect, name)`. `WhereNot(q => ...)` negates a group of conditions, and `WhereContains(column, text, folding)` matches text anywhere in a column with every pattern character escaped (`TextFolding.None`, `Database`, or `Invariant` for .NET's Unicode folding on SQLite with `SQLiteUnicodeText` registered).

For multipart table or schema names, `DbaIdentifierPath` provides the shared delimiter-aware split and unquote behavior used by bulk operations and table-copy planning.

## Query plan guard

`DBAClientX.QueryPlans` catches statements that read every row of a large table before they reach production.
`SQLite.ExplainQueryPlanAsync(database, sql, parameters)` returns the plan as `DbaQueryPlan` steps (operation, table,
index, temporary B-trees), and `QueryPlanAssert` checks it in any test framework:

```csharp
var plan = await sqlite.ExplainQueryPlanAsync("monitoring.db", sql, parameters);
QueryPlanAssert.NoFullScan(plan, "ProbeResults");          // throws QueryPlanViolationException with the SQL and plan
var result = QueryPlanAssert.Check(plan, new QueryPlanRules("ProbeResults")); // or inspect result.Violations
```

`SqlSargabilityAnalyzer.Analyze(sql)` is a heuristic over SQL text that reports functions wrapped around columns in
`WHERE`/`ON` conditions (`LOWER(ProbeName) = @p`, DbaClientX's `dbx_lower` included), a `COLLATE` applied to a comparison with a
column (`Name COLLATE NOCASE = @p`) and leading-wildcard patterns (`LIKE '%x'`). `Analyze(sql, options)` takes
`SqlSargabilityOptions`: other registered `Functions` and the collation each column's index uses (`ColumnCollations`,
`DefaultCollation` = `BINARY`), so a `COLLATE` that matches the index is not reported. Each finding names its `Column` and,
when the statement shows it, its `Table`. Confirm its findings with the plan. Plans depend on data and statistics, so check them on a database shaped like production (run `ANALYZE`).
Named large tables must appear in the plan by default (`RequireLargeTablesInPlan`), so a typo or an alias the guard
cannot resolve fails instead of passing.

A search can read most of a table too. Steps carry the rows the statistics expect (`EstimatedRows`, `TableRows`), and
`UsesIndexes`/`Check` report a `WideSearch` when a search reads more than `WideSearchFraction` (a tenth) of the rows, or
searches a range open on one side (`CompletedUtcMs < @cutoff`) that no `LIMIT` in index order stops; a temporary
B-tree that sorts such rows (a top-N `ORDER BY … LIMIT` over a non-selective key) is reported as well. A `MIN`/`MAX`
that reads one end of an index is a one-row search. Seed the statistics of a small test database so it plans like the
large one:

```csharp
await sqlite.WritePlannerStatisticsAsync("test.db", await sqlite.ReadPlannerStatisticsAsync("fixture-L.db"));
// or chosen values: a million rows, a third of them per Status value
await sqlite.WritePlannerStatisticsAsync("test.db", new[] { new SqlitePlannerStatistics("ProbeResults", "IX_Status", 1_000_000, new long[] { 333_334 }) });
```

`SqlStatementText.Split(script)` splits a script into statements to explain one at a time.

## Retry behavior

Provider clients use the same `TransientRetry` engine. `MaxRetryAttempts` includes the first attempt and `RetryDelay` is the exponential-backoff base. Connection establishment is retried separately from command execution.

Commands execute once by default (`CommandRetryMode.Never`), including queries, scalars, mapped results, reader startup and prepared statements. Returning rows does not make a command read-only: a batch, stored procedure or `INSERT ... RETURNING` can commit a write before a later statement fails. This replaces the automatic retries previously applied to result-returning commands.

Opt in on a dedicated client only when all its SQL and mapping callbacks are safe to repeat after partial success:

```csharp
using var reads = new DBAClientX.SQLite
{
    CommandRetryMode = DBAClientX.CommandRetryMode.ReplaySafe,
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromMilliseconds(100)
};
var total = await reads.ExecuteScalarAsync("app.db", "SELECT COUNT(*) FROM Users");
```

`RetryNonQueryOperations` remains available as a nonquery-only opt-in. Neither opt-in replays an individual command inside an explicit, ambient or library-owned transaction. SQLite also checks native transaction state for SQL `BEGIN` and `SAVEPOINT`. Roll back the failed transaction and decide whether the entire unit of work can be repeated. Streaming never replays rows after enumeration starts.

## Execution diagnostics

`DBAClientX.Diagnostics.DbaClientXDiagnostics` exposes an `ActivitySource` and a `Meter`, both named `DbaClientX`.
Subscribe through `ActivityListener`, `MeterListener`, or your telemetry collector. Ordinary queries, mapped results,
nonquery/scalar commands, SQLite session/prepared executions and reader startup share this contract across providers.
With no source or meter listener, execution scopes allocate no per-command state and do not hash statement text.

Activities use `DbaClientX.Command` and `DbaClientX.Connection.Open`. They report the provider, operation, outcome,
retry count, and known row count. Command fingerprints use the same SHA-256 of exact SQL text as
`DbaQueryExecutionException.QueryFingerprint`; literals are not normalized. SQL text, parameter values, connection
strings and exception messages are excluded. Fingerprints appear only in activities that request full data.

| Instrument | Unit | Meaning |
| --- | --- | --- |
| `dbaclientx.command.duration` | seconds | Logical execution, eligible retry delays and library-owned result consumption |
| `dbaclientx.command.count` | commands | One measurement per logical execution |
| `dbaclientx.command.retries` | retries | Eligible retries within that execution |
| `dbaclientx.command.rows` | rows | Materialized/mapped or emitted rows, or nonnegative provider-reported affected rows |
| `dbaclientx.connection.open.duration` | seconds | One native connection-open attempt, including provider pool wait |

Metric dimensions are limited to `db.system.name`, `dbaclientx.operation` and `dbaclientx.outcome`.
Providers are `mssql`, `postgresql`, `mysql`, `oracle`, `sqlite` or `other`. Outcomes are `success`, `error`, `canceled`
and `abandoned`. Scalars and unknown affected-row counts omit row measurements. Streaming counts delivered rows,
including partial results on failure; early disposal reports `abandoned`. Its duration includes time between caller
requests for rows. For `reader.open`, the command duration ends at reader handoff and excludes caller-owned consumption.
Connection-open duration excludes pragmas, configuration callbacks, transaction startup and retry delays.
Activity and meter subscriber failures do not replace database results or errors.

```csharp
using System.Diagnostics.Metrics;
using DBAClientX.Diagnostics;

using var listener = new MeterListener
{
    InstrumentPublished = (instrument, owner) =>
    {
        if (instrument.Meter.Name == DbaClientXDiagnostics.MeterName &&
            instrument.Name == "dbaclientx.command.duration")
            owner.EnableMeasurementEvents(instrument);
    }
};
listener.SetMeasurementEventCallback<double>((instrument, seconds, tags, state) =>
    Console.WriteLine($"Command duration: {seconds:F6}s"));
listener.Start();
```

For a caller-owned native `DbConnection`, `DbaClientXDiagnostics.OpenConnection` and `OpenConnectionAsync` observe the
provider open without taking ownership, retrying or applying configuration. Provider clients retain their existing
virtual open hooks. Telemetry observes execution and does not change replay, cancellation or transaction policy.

## Notes
- Ship a per-provider package alongside Core for ADO.NET specifics (see provider READMEs).
- `DbParameterMapper` supports dotted paths and ambient values.
