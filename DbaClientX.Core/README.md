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

`SelectRaw`, `FromRaw`, `JoinRaw`, `WhereRaw`, `GroupByRaw`, `HavingRaw`, and `OrderByRaw` emit caller-authored SQL. Never pass user input to these methods. The legacy two-string join overloads remain available for migration but are obsolete because they treat both arguments as raw SQL. Comparison operators are limited to the supported safe operator set, and `Limit`, `Offset`, and `Top` reject negative values.

## Notes
- Ship a per-provider package alongside Core for ADO.NET specifics (see provider READMEs).
- `DbParameterMapper` supports dotted paths and ambient values.
