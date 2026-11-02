Import-Module DbaClientX

# This example creates a local database and a table for its sample records.
$database = Join-Path $PWD 'example-users.db'
Invoke-DbaXSQLite -Database $database -Query @'
CREATE TABLE IF NOT EXISTS ExampleUsers (Id INTEGER PRIMARY KEY, Name TEXT, Role TEXT);
'@ -ErrorAction Stop

# Replace the sample rows by key so the example can be run again.
Invoke-DbaXSQLite -Database $database -Query @'
INSERT INTO ExampleUsers (Id, Name, Role) VALUES
    (1, 'Alice', 'Admin'), (2, 'Bob', 'Reader'), (3, 'Carol', 'Admin')
ON CONFLICT(Id) DO UPDATE SET Name = excluded.Name, Role = excluded.Role;
'@ -ErrorAction Stop

# SQLite filters the rows; only administrators enter the PowerShell pipeline.
Invoke-DbaXSQLite -Database $database `
    -Query 'SELECT Name, Role FROM ExampleUsers WHERE Role COLLATE BINARY = @Role ORDER BY Name' `
    -Parameters @{ Role = 'Admin' } |
    Select-Object Name, Role

# PowerShell filters the rows after SQLite returns every user's name and role.
Invoke-DbaXSQLite -Database $database `
    -Query 'SELECT Name, Role FROM ExampleUsers ORDER BY Name' |
    Where-Object Role -CEQ 'Admin' |
    Select-Object Name, Role

# On PowerShell 7, -Stream avoids buffering the query result. It does not move
# downstream Where-Object filtering into SQLite. PowerShell 5.1 uses buffered queries.
