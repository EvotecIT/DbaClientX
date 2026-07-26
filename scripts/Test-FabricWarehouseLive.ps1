[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string] $Endpoint,

    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string] $Warehouse,

    [ValidateSet('ActiveDirectoryDefault', 'ActiveDirectoryInteractive', 'ActiveDirectoryManagedIdentity')]
    [string] $Authentication = 'ActiveDirectoryDefault'
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$moduleManifest = Join-Path $repositoryRoot 'Module\DbaClientX.psd1'
$tableName = "DbaClientXFabricProbe_$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$qualifiedTable = "dbo.$tableName"

if (-not $PSCmdlet.ShouldProcess(
        "$Endpoint / $Warehouse / $qualifiedTable",
        'Create, validate, and remove a temporary Fabric Warehouse table')) {
    return
}

$originalDevelopmentBinarySetting = $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES
try {
    $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
    Import-Module $moduleManifest -Force -ErrorAction Stop

    $connectionString = [DBAClientX.FabricWarehouseProfile]::BuildConnectionString(
        $Endpoint,
        $Warehouse,
        30,
        'DbaClientX Fabric Warehouse live validation')
    $builder = [Microsoft.Data.SqlClient.SqlConnectionStringBuilder]::new($connectionString)
    $builder.Authentication = [Microsoft.Data.SqlClient.SqlAuthenticationMethod] [Enum]::Parse(
        [Microsoft.Data.SqlClient.SqlAuthenticationMethod],
        $Authentication,
        $false)
    $connectionString = $builder.ConnectionString

    $client = [DBAClientX.SqlServer]::new()
    $client.ConnectionOptions = [DBAClientX.SqlServerConnectionOptions]@{
        CompatibilityProfile = [DBAClientX.SqlServerCompatibilityProfile]::FabricWarehouse
    }

    $created = $false
    try {
        $client.ExecuteNonQuery(
            $connectionString,
            "CREATE TABLE [dbo].[$tableName] ([Id] int NOT NULL, [Name] varchar(100) NULL);") | Out-Null
        $created = $true

        $rows = [Data.DataTable]::new('FabricRows')
        $rows.Columns.Add('Id', [int]) | Out-Null
        $rows.Columns.Add('Name', [string]) | Out-Null
        $rows.Rows.Add(1, 'One') | Out-Null
        $rows.Rows.Add(2, 'Two') | Out-Null
        $rows.Rows.Add(3, 'Three') | Out-Null

        $client.BulkInsert($connectionString, $rows, $qualifiedTable, $false, 1000, 120)
        $count = [long] $client.ExecuteScalar(
            $connectionString,
            "SELECT COUNT_BIG(*) FROM [dbo].[$tableName];")
        $table = $client.GetTables($connectionString, 'dbo', $false) |
            Where-Object Name -EQ $tableName |
            Select-Object -First 1
        $columns = @($client.GetColumns($connectionString, 'dbo', $tableName))
        $readBack = $client.Query(
            $connectionString,
            "SELECT [Id], [Name] FROM [dbo].[$tableName] ORDER BY [Id];")

        if ($count -ne 3 -or $null -eq $table -or $columns.Count -ne 2) {
            throw 'Fabric Warehouse validation did not return the expected table, columns, and row count.'
        }

        [pscustomobject]@{
            EndpointValidated = $true
            Warehouse         = $Warehouse
            Table             = $qualifiedTable
            RowsWritten       = 3
            RowsRead          = @($readBack).Count
            ColumnsDiscovered = $columns.Count
            BulkCopyPreview   = [DBAClientX.FabricWarehouseProfile]::DirectBulkCopyIsPreview
        }
    } finally {
        if ($created) {
            $client.ExecuteNonQuery(
                $connectionString,
                "DROP TABLE [dbo].[$tableName];") | Out-Null
        }
        $client.Dispose()
    }
} finally {
    $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = $originalDevelopmentBinarySetting
}
