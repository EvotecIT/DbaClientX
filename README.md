## DbaClientX

DbaClientX is a lightweight, multi‑provider database client for .NET and PowerShell.
It offers:

- Providers: SQL Server, PostgreSQL, MySQL, SQLite, Oracle
- Sync/async APIs with cancellation, retries, and transactions
- A small SQL query builder with dialect support and parameterization
- PowerShell cmdlets for quick scripting

### Repo structure

- `DbaClientX.Core` – shared base (`DatabaseClientBase`), retry logic, query builder
- `DbaClientX.SqlServer` / `PostgreSql` / `MySql` / `SQLite` / `Oracle` – providers
- `DbaClientX.PowerShell` – PowerShell cmdlets
- `DbaClientX.Tests` – xUnit unit tests
- `Module` – Pester tests and module build script

### Build & test

```
dotnet restore DbaClientX.sln
dotnet build DbaClientX.sln -c Release
dotnet test DbaClientX.sln -c Release --framework net8.0
```

### Release packaging (manual signing)

Package publishing is intentionally manual in this repository because releases are signed locally with the USB key certificate.

1. Generate a build plan:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -Plan $true
```

2. Build signed packages locally:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -Build $true -PublishNuget $false -PublishGitHub $false
```

3. Publish packages only when explicitly intended:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -PublishNuget $true
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -PublishGitHub $true
```

Build configuration lives in `Build/project.build.json` and artifacts are generated under `Artefacts/ProjectBuild`.

### Notes

- The solution enables nullable reference types and .NET analyzers via `Directory.Build.props`.
- SourceLink is enabled for all projects for better debugging into packages.
- SQL Server provider uses `Microsoft.Data.SqlClient`.

