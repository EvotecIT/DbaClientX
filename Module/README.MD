# DbaClientX PowerShell Module

The PowerShell module now includes transaction wrapper functions that follow the same safer lifecycle pattern as the .NET clients:

- `Invoke-DbaXTransaction`
- `Invoke-DbaXMySqlTransaction`
- `Invoke-DbaXPostgreSqlTransaction`
- `Invoke-DbaXOracleTransaction`
- `Invoke-DbaXSQLiteTransaction`

Each helper:

- honors `-WhatIf` and `-Confirm`
- validates connection details before starting work
- begins a transaction, runs your script block, commits on success
- rolls back on failure when the transaction is still active
- disposes the client in `finally`

Example:

```powershell
Invoke-DbaXTransaction -Server 'sql01' -Database 'App' -ScriptBlock {
    param($client)

    $client.ExecuteNonQuery('sql01', 'App', $true, 'UPDATE dbo.Users SET IsActive = 1 WHERE Id = 1', $null, $true)
    $client.Query('sql01', 'App', $true, 'SELECT @@TRANCOUNT', $null, $true)
}
```
