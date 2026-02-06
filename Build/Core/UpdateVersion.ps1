param(
    [Parameter(Mandatory = $true)]
    [string] $NewVersion
)

Import-Module PSPublishModule -Force -ErrorAction Stop

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$excludeFolders = @(
    (Join-Path $repoRoot 'Build\Artefacts'),
    (Join-Path $repoRoot 'Module\Artefacts')
)

Get-ProjectVersion -Path $repoRoot -ExcludeFolders $excludeFolders | Format-Table
Set-ProjectVersion -Path $repoRoot -NewVersion $NewVersion -WhatIf:$false -Verbose -ExcludeFolders $excludeFolders | Format-Table
