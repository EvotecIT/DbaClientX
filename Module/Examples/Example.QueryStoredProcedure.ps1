Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

Invoke-DbaXQuery -StoredProcedure "dbo.MyProcedure" -Server "SQL1" -Database "master"

