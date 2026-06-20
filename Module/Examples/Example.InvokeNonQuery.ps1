Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_SQLSERVER
if ([string]::IsNullOrWhiteSpace($server)) {
    $server = 'localhost'
}

$database = $env:DBACLIENTX_SQLDATABASE
if ([string]::IsNullOrWhiteSpace($database)) {
    $database = 'master'
}

$sql = @'
CREATE TABLE #DbaClientXDemo
(
    Id int NOT NULL,
    Name nvarchar(50) NOT NULL
);

INSERT INTO #DbaClientXDemo (Id, Name)
VALUES (1, N'Alpha'), (2, N'Beta');
'@

$affectedRows = Invoke-DbaXNonQuery `
    -Server $server `
    -Database $database `
    -TrustServerCertificate `
    -Query $sql

"Rows affected: $affectedRows"
