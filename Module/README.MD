# DbaClientX PowerShell Module

Install the module, import it, and use the cmdlets from regular scripts, scheduled jobs, or migration tools.

```powershell
Install-Module DbaClientX -Scope CurrentUser
Import-Module DbaClientX
```

## Run Several SQL Server Commands In One Transaction

Use `Invoke-DbaXTransaction` when the script block should commit only after every command succeeds. If an exception is thrown while the transaction is still active, DbaClientX rolls it back and disposes the client.

```powershell
Invoke-DbaXTransaction -Server 'sql01' -Database 'App' -ScriptBlock {
    param($client)

    $client.ExecuteNonQuery(
        'sql01',
        'App',
        $true,
        'UPDATE dbo.Users SET IsActive = 1 WHERE Id = 1',
        $null,
        $true)

    $client.Query(
        'sql01',
        'App',
        $true,
        'SELECT @@TRANCOUNT AS OpenTransactions',
        $null,
        $true)
}
```

Use the provider-specific transaction helpers when the target is not SQL Server. They follow the same script-block lifecycle:

- `Invoke-DbaXMySqlTransaction`
- `Invoke-DbaXOracleTransaction`
- `Invoke-DbaXSQLiteTransaction`
- `Invoke-DbaXPostgreSqlTransaction`

## Move Rows Into Tables

Use `Write-DbaXTableData` when you already have rows in PowerShell and want the provider package to own the bulk write. SQL Server `IDataReader` input streams directly into `SqlBulkCopy`; when passing a reader through `-InputObject`, wrap it as `-InputObject (, $reader)` so PowerShell treats it as one object:

```powershell
$rows = 1..100 | ForEach-Object {
    [pscustomobject]@{
        Id = $_
        DisplayName = "Row $_"
    }
}

$rows | Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=.;Database=tempdb;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ImportRows' `
    -AutoCreateTable `
    -BatchSize 5000 `
    -PassThru
```

Use `Copy-DbaXTableData` when the source and destination are both database tables:

```powershell
Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\history.db' `
    -SourceTable 'ProbeResults' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=.;Database=History;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ProbeResults' `
    -OrderBy Id `
    -BatchSize 5000 `
    -PassThru
```

## Pair With PSWriteOffice For CSV And Excel

Use PSWriteOffice for the file format and DbaClientX for the database write:

```powershell
$reader = $null
try {
    $reader = Import-OfficeCsv .\Users.csv -AsDataReader -InferSchema
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=tempdb;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -InputObject (, $reader) `
        -AutoCreateTable `
        -BatchSize 5000
} finally {
    if ($null -ne $reader) {
        $reader.Dispose()
    }
}

Import-OfficeExcel .\Users.xlsx -AsDataTable |
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=tempdb;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsersFromExcel' `
        -AutoCreateTable `
        -BatchSize 5000
```

For full workflow proof, run `Module/Examples/Example.CsvRoundTrip.ps1`, `Module/Examples/Example.ExcelRoundTrip.ps1`, or `Module/Examples/Benchmark.OfficeFileRoundTrip.ps1`.

The scripts in `Module/Examples` are runnable how-to examples. They use `Import-Module DbaClientX` and public cmdlet parameters, not local build-output paths.
