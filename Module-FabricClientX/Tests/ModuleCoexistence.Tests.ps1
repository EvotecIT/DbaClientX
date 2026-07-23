Describe 'Packaged DbaClientX and FabricClientX coexistence' -Tag 'PackagedCoexistence' {
    It 'loads both brands in the requested order and keeps their command surfaces usable' {
        if ($env:FABRICCLIENTX_REQUIRE_COEXISTENCE_SMOKE -ne 'true') {
            Set-ItResult -Skipped -Because 'packaged coexistence smoke was not requested'
            return
        }

        $order = $env:FABRICCLIENTX_IMPORT_ORDER
        $order | Should -BeIn @('DbaFirst', 'FabricFirst')

        $dbaManifest = [IO.Path]::GetFullPath(
            (Join-Path $PSScriptRoot '..\..\Module\Artefacts\Unpacked\DbaClientX\DbaClientX.psd1'))
        $fabricManifest = [IO.Path]::GetFullPath(
            (Join-Path $PSScriptRoot '..\Artefacts\Unpacked\FabricClientX\FabricClientX.psd1'))
        Test-Path -LiteralPath $dbaManifest | Should -BeTrue
        Test-Path -LiteralPath $fabricManifest | Should -BeTrue

        $manifests = if ($order -eq 'DbaFirst') {
            @($dbaManifest, $fabricManifest)
        } else {
            @($fabricManifest, $dbaManifest)
        }

        $importOutput = foreach ($manifest in $manifests) {
            Import-Module $manifest -Force -ErrorAction Stop 3>&1
        }
        @($importOutput | Where-Object { $_ -is [System.Management.Automation.WarningRecord] }) |
            Should -BeNullOrEmpty

        $dbaCommands = @(Get-Command -Module DbaClientX)
        $fabricCommands = @(Get-Command -Module FabricClientX)
        $dbaCommands.Count | Should -Be 37
        $fabricCommands.Count | Should -Be 8
        @($dbaCommands.Name | Where-Object { $_ -match 'Fabric|PowerBI' }) | Should -BeNullOrEmpty
        @($fabricCommands.Name | Where-Object { $_ -match 'DbaX' }) | Should -BeNullOrEmpty

        $connectionString = New-DbaXConnectionString -Provider SQLite -Database ':memory:'
        $connectionString | Should -Match ':memory:'

        $token = [Security.SecureString]::new()
        foreach ($character in 'coexistence-test-token'.ToCharArray()) {
            $token.AppendChar($character)
        }
        $token.MakeReadOnly()
        $warehouseOptions = New-FabricXWarehouseConnectionOptions `
            -AccessToken $token `
            -ExpiresOn ([DateTimeOffset]::UtcNow.AddHours(1))
        $warehouseOptions.GetType().FullName | Should -Be 'DBAClientX.SqlServerConnectionOptions'
        $warehouseOptions.CompatibilityProfile.ToString() | Should -Be 'FabricWarehouse'

        if ($IsCoreCLR) {
            $dbaCommand = Get-Command Invoke-DbaXQuery
            $fabricCommand = Get-Command Get-FabricXWorkspace
            $dbaContext = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(
                $dbaCommand.ImplementingType.Assembly)
            $fabricContext = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(
                $fabricCommand.ImplementingType.Assembly)

            $dbaContext.Name | Should -Be 'DbaClientX'
            $fabricContext.Name | Should -Be 'FabricClientX'
            [object]::ReferenceEquals($dbaContext, $fabricContext) | Should -BeFalse
        }
    }
}
