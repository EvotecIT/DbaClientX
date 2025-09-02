Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$cs = [DBAClientX.Oracle]::BuildConnectionString('OracleServer', 'ORCL', 'user', 'pass')
Write-Host $cs

$oracle = [DBAClientX.Oracle]::new()
try {
    $oracle.BeginTransaction('OracleServer', 'ORCL', 'user', 'pass')
    $rows = $oracle.ExecuteNonQuery('OracleServer', 'ORCL', 'user', 'pass', 'UPDATE Users SET Disabled = 1 WHERE 1=0', $null, $true)
    $oracle.Commit()
    $rows
} catch {
    $oracle.Rollback()
    throw
} finally {
    $oracle.Dispose()
}
