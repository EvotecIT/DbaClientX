# DbaClientX.Core

Core building blocks for DbaClientX: retryable execution pipeline, parameter mapping for POCOs/dictionaries, SQL query builder/compiler, and light utilities.

- Target Frameworks: `net8.0`, `net472`
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
    providerAlias: "sqlserver",          // mssql|sqlserver|postgres|pgsql|mysql|sqlite|oracle
    connectionString: "Server=.;Database=App;Trusted_Connection=True;",
    sql: "INSERT INTO Users(Id,Name) VALUES(@Id,@Name)",
    items: items,
    map: map,
    options: new DbParameterMapperOptions { DateTimeOffsetAsUtcDateTime = true }
);
```

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

## Notes
- Ship a per-provider package alongside Core for ADO.NET specifics (see provider READMEs).
- `DbParameterMapper` supports dotted paths and ambient values.

