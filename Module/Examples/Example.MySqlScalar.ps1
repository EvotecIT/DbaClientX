Invoke-DbaXMySqlScalar -Server 'mysqlsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query "SELECT COUNT(*) FROM Users"
