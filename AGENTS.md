# DbaClientX

C# database client libraries for SQL Server, PostgreSQL, MySQL, SQLite, Oracle,
Azure Tables and DBF files, with a provider-neutral core and the `DbaClientX`
PowerShell module. The repository also incubates FabricClientX (Microsoft
Fabric and Power BI automation), a separate brand with its own packages and module.

## Layout

- `DbaClientX.Core/`: shared abstractions, query builder, provider-neutral table
  planning and data movement.
- Providers: `DbaClientX.SqlServer/`, `DbaClientX.PostgreSql/`,
  `DbaClientX.MySql/`, `DbaClientX.SQLite/`, `DbaClientX.Oracle/`,
  `DbaClientX.AzureTables/`, `DbaClientX.Dbf/`.
- `DbaClientX.PowerShell/` and `Module/`: binary cmdlets, manifest, build,
  Pester tests and generated docs.
- `FabricClientX.Core/`, `FabricClientX.PowerBI/`, `FabricClientX.OfficeIMO/`,
  `FabricClientX.PowerShell/` and `Module-FabricClientX/`: the FabricClientX
  family and its module.
- `PowerShell.Shared/`: async cmdlet base shared by both modules.
- Tests: `DbaClientX.Tests/`, `DbaClientX.AzureTables.Tests/`,
  `DbaClientX.Dbf.Tests/`, `FabricClientX.Tests/` (xUnit), `Module/Tests/`
  and `Module-FabricClientX/Tests/` (Pester).
- `DbaClientX.SQLite.AotSmoke/`, `DbaClientX.Benchmarks/`, `DbaClientX.Examples/`.
- `docs/`: architecture boundaries and design notes. `scripts/`: live Fabric checks.

## Build and test

```powershell
dotnet build DbaClientX.sln -c Release
dotnet test DbaClientX.sln -c Release --no-build
./Module/DbaClientX.Tests.ps1                         # DbaClientX module Pester tests
./Module-FabricClientX/FabricClientX.Tests.ps1        # FabricClientX module Pester tests
./Module/Build/Build-Module.ps1 -RunMode Build -SignModule $false
./Module-FabricClientX/Build/Build-Module.ps1 -RunMode Build -SignModule $false
```

- Targets: providers and core `net472;net8.0;net10.0` (`net472` only on
  Windows); `DbaClientX.Dbf` adds `netstandard2.0`; cmdlets `net472;net8.0`;
  tests `net8.0;net10.0`. SDK pinned in `global.json` (10.0.100).
- Central package versions live in `Directory.Packages.props`. Package
  validation runs against the published 1.0.8 baseline during pack
  (`Directory.Build.targets`).
- NuGet packages and the module version and publish together through
  `Module/Build/Build-Module.ps1`; `Build/Build-Project.ps1` refuses to version or
  publish on its own. FabricClientX uses `Build/fabricclientx.build.json`.
- Release only: `-RunMode Publish` signs and publishes.

## Generated files

- `Module/Docs/*.md`, `Module/en-US/` and the FabricClientX equivalents come
  from the cmdlet XML docs. Regenerate with `-RunMode Documentation`.
- `Module/DbaClientX.psm1`, `Module-FabricClientX/FabricClientX.psm1` and the
  root `DbaClientX.psm1` are PowerForge bootstrappers. Don't edit them.

## Conventions

- Keep changes portable across SQL Server, PostgreSQL, Oracle, MySQL and
  SQLite. Cross-provider changes need SQL Server tests plus the affected
  providers' tests.
- Follow the dependency rules in `docs/architecture-boundaries.md`:
  providers depend on `DbaClientX.Core`; FabricClientX never depends on a
  database provider; cmdlets bind parameters and project results but don't own
  workflows; no forwarding aliases or duplicate code between the two modules.
- Public examples describe the published package shape, not local paths.

## Gotchas

- Live database tests skip unless their connection variable is set, for example
  `DBACLIENTX_SQLSERVER_TEST_CONNECTION`, `DBACLIENTX_POSTGRESQL_TEST_CONNECTION`,
  `DBACLIENTX_MYSQL_TEST_CONNECTION` and `DBACLIENTX_ORACLE_TEST_CONNECTION`.
  CI runs PostgreSQL and MySQL in service containers.
- Destructive SQL Server suites (backup, recovery, corruption, long waits) have
  their own `DBACLIENTX_SQL_*` switches. Don't point them at a real server.
- `scripts/Test-FabricWarehouseLive.ps1` and `scripts/Test-FabricPowerBILive.ps1`
  need a Fabric tenant. Run them only when the task asks.
- Set `DBACLIENTX_USE_DEVELOPMENT_BINARIES=true` to load the module from
  `DbaClientX.PowerShell/bin` instead of packaged binaries.
