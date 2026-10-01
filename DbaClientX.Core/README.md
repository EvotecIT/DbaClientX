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
`DefaultCollation` = `BINARY`), so a `COLLATE` that matches the index is not reported. Confirm its findings with the plan. Plans depend on data and statistics, so check them on a database shaped like production (run `ANALYZE`).
Named large tables must appear in the plan by default (`RequireLargeTablesInPlan`), so a typo or an alias the guard
cannot resolve fails instead of passing.

## Retry behavior

Provider clients and streaming-reader startup use the same `TransientRetry` engine. `MaxRetryAttempts` includes the first attempt, `RetryDelay` is the exponential-backoff base, and non-query retries remain disabled by default to avoid replaying a write that may already have succeeded.

## Notes
- Ship a per-provider package alongside Core for ADO.NET specifics (see provider READMEs).
- `DbParameterMapper` supports dotted paths and ambient values.
