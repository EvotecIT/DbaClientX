---
Module Name: DbaClientX
Module Guid: c22cc272-c829-49e2-aaa1-58d3c36edb94
Download Help Link: https://github.com/EvotecIT/DbaClientX
Help Version: 1.0.6
Locale: en-US
---
# DbaClientX Module
## Description
Simple project to query Sql Server and other databases using PowerShell

## DbaClientX Cmdlets
### [ConvertTo-DbaXParameterMap](ConvertTo-DbaXParameterMap.md)
Converts objects into provider parameter dictionaries using the DbaClientX parameter mapper.

### [Copy-DbaXAzureTableData](Copy-DbaXAzureTableData.md)
Copies Azure Table data between storage accounts or Table API endpoints.

### [Copy-DbaXTableData](Copy-DbaXTableData.md)
Copies table data from one DbaClientX provider connection to another using paged reads and provider-native bulk writes.

### [Get-DbaXAzureTableEntity](Get-DbaXAzureTableEntity.md)
Reads Azure Table entities with native continuation-token paging.

### [Get-DbaXMetadata](Get-DbaXMetadata.md)
Gets database metadata without requiring SQL Server Management Objects.

### [Get-DbaXProviderCapability](Get-DbaXProviderCapability.md)
Gets the capabilities exposed by DbaClientX providers.

### [Get-DbaXSQLiteDiagnostics](Get-DbaXSQLiteDiagnostics.md)
Collects SQLite file and database diagnostics through the DbaClientX SQLite provider.

### [Get-DbaXSqlServerManagement](Get-DbaXSqlServerManagement.md)
Gets SQL Server-specific management metadata without requiring SQL Server Management Objects.

### [Get-DbaXSqlServerMonitoring](Get-DbaXSqlServerMonitoring.md)
Collects a SQL Server monitoring snapshot through the DbaClientX SQL Server provider.

### [Get-DbaXTableCopyPlan](Get-DbaXTableCopyPlan.md)
Discovers provider metadata and builds a DbaClientX table-copy plan.

### [Invoke-DbaXBulkInsert](Invoke-DbaXBulkInsert.md)
Invokes a provider-native DbaClientX bulk insert from tabular PowerShell input.

### [Invoke-DbaXMySql](Invoke-DbaXMySql.md)
Invokes commands against a MySQL database.

### [Invoke-DbaXMySqlNonQuery](Invoke-DbaXMySqlNonQuery.md)
Executes a non-query SQL command against MySQL.

### [Invoke-DbaXMySqlScalar](Invoke-DbaXMySqlScalar.md)
Executes a scalar SQL query against MySQL.

### [Invoke-DbaXMySqlTransaction](Invoke-DbaXMySqlTransaction.md)
Runs a script block inside a MySQL transaction.

### [Invoke-DbaXNonQuery](Invoke-DbaXNonQuery.md)
Executes a non-query SQL command against SQL Server.

### [Invoke-DbaXOracle](Invoke-DbaXOracle.md)
Invokes commands against an Oracle database.

### [Invoke-DbaXOracleNonQuery](Invoke-DbaXOracleNonQuery.md)
Executes a non-query SQL command against Oracle.

### [Invoke-DbaXOracleScalar](Invoke-DbaXOracleScalar.md)
Executes a scalar SQL query against Oracle.

### [Invoke-DbaXOracleTransaction](Invoke-DbaXOracleTransaction.md)
Runs a script block inside an Oracle transaction.

### [Invoke-DbaXPostgreSql](Invoke-DbaXPostgreSql.md)
Invokes commands against a PostgreSQL database.

### [Invoke-DbaXPostgreSqlNonQuery](Invoke-DbaXPostgreSqlNonQuery.md)
Executes a non-query SQL command against PostgreSQL.

### [Invoke-DbaXPostgreSqlTransaction](Invoke-DbaXPostgreSqlTransaction.md)
Runs a script block inside a PostgreSQL transaction.

### [Invoke-DbaXQuery](Invoke-DbaXQuery.md)
Invokes a SQL Server query or stored procedure.

### [Invoke-DbaXQueryStream](Invoke-DbaXQueryStream.md)
Streams query rows through a DbaClientX provider.

### [Invoke-DbaXSQLite](Invoke-DbaXSQLite.md)
Invokes a query against a SQLite database.

### [Invoke-DbaXSQLiteMaintenance](Invoke-DbaXSQLiteMaintenance.md)
Runs SQLite maintenance operations through the DbaClientX SQLite provider.

### [Invoke-DbaXSQLiteTransaction](Invoke-DbaXSQLiteTransaction.md)
Runs a script block inside a SQLite transaction.

### [Invoke-DbaXStoredProcedure](Invoke-DbaXStoredProcedure.md)
Invokes a stored procedure through a DbaClientX provider.

### [Invoke-DbaXTransaction](Invoke-DbaXTransaction.md)
Runs a script block inside a SQL Server transaction.

### [New-DbaXConnectionString](New-DbaXConnectionString.md)
Builds a provider connection string using the matching DbaClientX C# provider.

### [New-DbaXQuery](New-DbaXQuery.md)
Creates SQL query-builder objects using the DbaClientX core query builder.

### [New-DbaXTableCopyDefinition](New-DbaXTableCopyDefinition.md)
Creates a DbaClientX table-copy definition.

### [New-DbaXTableCopyPlan](New-DbaXTableCopyPlan.md)
Builds a table-copy plan from supplied DbaClientX metadata objects.

### [Test-DbaXConnection](Test-DbaXConnection.md)
Validates and optionally pings a DbaClientX provider connection string.

### [Write-DbaXAzureTableEntity](Write-DbaXAzureTableEntity.md)
Writes PowerShell pipeline objects to Azure Tables in partition-safe transactions.

### [Write-DbaXTableData](Write-DbaXTableData.md)
Writes tabular pipeline input to a database table using provider-native bulk insert APIs.
