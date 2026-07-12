# DbaClientX.Core

Core building blocks for DbaClientX: retryable execution pipeline, parameter mapping for POCOs/dictionaries, SQL query builder/compiler, and light utilities.

- Target Frameworks: `net472`, `net8.0`, `net10.0`
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

## Query builder (string-safe, provider-aware quoting)

```csharp
using DBAClientX.QueryBuilder;

var sql = QueryBuilder
    .Select("Id", "Name")
    .From("dbo.Users")
    .Where("IsActive", true)
    .OrderBy("Name")
    .Compile(SqlDialect.SqlServer);
```

For multipart table or schema names, `DbaIdentifierPath` provides the shared delimiter-aware split and unquote behavior used by bulk operations and table-copy planning.

## Retry behavior

Provider clients and streaming-reader startup use the same `TransientRetry` engine. `MaxRetryAttempts` includes the first attempt, `RetryDelay` is the exponential-backoff base, and non-query retries remain disabled by default to avoid replaying a write that may already have succeeded.

## Notes
- Ship a per-provider package alongside Core for ADO.NET specifics (see provider READMEs).
- `DbParameterMapper` supports dotted paths and ambient values.
