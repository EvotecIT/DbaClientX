Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force -Verbose

# Build query with modern pagination
$query = New-DbaXQuery -TableName users -Compile -Limit 5 -Offset 2
$query
