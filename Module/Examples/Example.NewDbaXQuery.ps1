Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force -Verbose

# Build query with modern pagination
$query = New-DbaXQuery -TableName users -Compile -Limit 5 -Offset 2
$query

# Invalid pagination values emit warnings and can be converted to errors
try {
    New-DbaXQuery -TableName users -Compile -Limit -1 -Offset -2 -ErrorAction Stop
} catch {
    Write-Host "Caught exception: $($_.Exception.Message)"
}
