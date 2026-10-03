$env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXSQLiteMaintenance -Action Backup' {
    BeforeAll {
        $script:ModuleManifest = (Resolve-Path -LiteralPath "$PSScriptRoot/../DbaClientX.psd1").Path

        function New-BackupTestDatabase {
            param(
                [Parameter(Mandatory)][ValidateSet('WAL', 'DELETE')][string] $JournalMode,
                [int] $Rows = 64
            )

            $path = Join-Path ([IO.Path]::GetTempPath()) "dbaclientx-ps-backup-$([guid]::NewGuid().ToString('N')).sqlite"
            $sqlite = [DBAClientX.SQLite]::new()
            try {
                $null = $sqlite.ExecuteNonQuery($path, "PRAGMA journal_mode = $JournalMode;")
                $null = $sqlite.ExecuteNonQuery($path, 'CREATE TABLE backup_contract(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);')
                $null = $sqlite.ExecuteNonQuery($path, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < $Rows) INSERT INTO backup_contract(payload) SELECT randomblob(4096) FROM n;")
            } finally {
                $sqlite.Dispose()
            }
            $path
        }

        function Get-BackupTestRowCount {
            param([Parameter(Mandatory)][string] $Path)

            $sqlite = [DBAClientX.SQLite]::new()
            try {
                [long] $sqlite.ExecuteScalar($Path, 'SELECT COUNT(*) FROM backup_contract;')
            } finally {
                $sqlite.Dispose()
            }
        }

        function Get-SqliteProviderType {
            param([Parameter(Mandatory)][string] $Name)

            # The provider can load into the module's own load context on PowerShell 7, where type literals do not resolve.
            $assembly = [AppDomain]::CurrentDomain.GetAssemblies() |
                Where-Object { $_.GetName().Name -eq 'Microsoft.Data.Sqlite' } |
                Select-Object -First 1
            if ($assembly) {
                $assembly.GetType($Name, $true)
            }
        }

        function Remove-BackupTestDatabase {
            param([string[]] $Path)

            $connectionType = Get-SqliteProviderType -Name 'Microsoft.Data.Sqlite.SqliteConnection'
            if ($connectionType) {
                $connectionType.GetMethod('ClearAllPools').Invoke($null, $null)
            }
            foreach ($file in $Path) {
                foreach ($suffix in '', '-wal', '-shm', '-journal') {
                    Remove-Item -LiteralPath "$file$suffix" -Force -ErrorAction SilentlyContinue
                }
            }
        }

        function New-ModuleRunspacePowerShell {
            $state = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
            $state.ImportPSModule([string[]] @($script:ModuleManifest))
            [System.Management.Automation.PowerShell]::Create($state)
        }
    }

    It 'uses <Expected> for a <JournalMode> database when -BackupMethod is <Method>' -TestCases @(
        @{ JournalMode = 'WAL'; Method = 'Auto'; Expected = 'Snapshot' }
        @{ JournalMode = 'DELETE'; Method = 'Auto'; Expected = 'Incremental' }
        @{ JournalMode = 'WAL'; Method = 'Incremental'; Expected = 'Incremental' }
        @{ JournalMode = 'DELETE'; Method = 'Snapshot'; Expected = 'Snapshot' }
    ) {
        param($JournalMode, $Method, $Expected)

        $source = New-BackupTestDatabase -JournalMode $JournalMode
        $destination = "$source.backup"
        try {
            $result = Invoke-DbaXSQLiteMaintenance -Database $source -Action Backup -Destination $destination -BackupMethod $Method -PassThru -Confirm:$false

            $result.Completed | Should -BeTrue
            $result.BackupMethod.ToString() | Should -Be $Expected
            [IO.Path]::IsPathRooted($result.Destination) | Should -BeTrue
            [IO.Path]::GetFileName($result.Destination) | Should -Be ([IO.Path]::GetFileName($destination))
            $result.CopiedPages | Should -BeGreaterThan 0
            Get-BackupTestRowCount -Path $result.Destination | Should -Be 64
            Get-BackupTestRowCount -Path $destination | Should -Be 64
        } finally {
            Remove-BackupTestDatabase -Path $source, $destination
        }
    }

    It 'reports a failed copy as a query execution error without the paths' {
        # The destination folder is a file, so the copy fails with a file-system error that names the path.
        $source = New-BackupTestDatabase -JournalMode WAL
        $blockingFile = "$source.parent"
        $destination = Join-Path $blockingFile 'backup.sqlite'
        [IO.File]::WriteAllText($blockingFile, 'not a directory')
        try {
            $failure = $null
            try {
                Invoke-DbaXSQLiteMaintenance -Database $source -Action Backup -Destination $destination -Confirm:$false -ErrorAction Stop
            } catch {
                $failure = $_
            }

            $failure | Should -Not -BeNullOrEmpty
            $failure.Exception | Should -BeOfType ([DBAClientX.DbaQueryExecutionException])
            $failure.Exception.ProviderExceptionType | Should -Be 'System.IO.IOException'
            $failure.Exception.Message | Should -Not -BeLike "*$([IO.Path]::GetFileName($source))*"
        } finally {
            Remove-BackupTestDatabase -Path $source, $blockingFile
        }
    }

    It 'reports progress and completes it' {
        $source = New-BackupTestDatabase -JournalMode WAL -Rows 512
        $destination = "$source.backup"
        $ps = New-ModuleRunspacePowerShell
        try {
            $null = $ps.AddCommand('Invoke-DbaXSQLiteMaintenance')
            $null = $ps.AddParameter('Database', $source)
            $null = $ps.AddParameter('Action', 'Backup')
            $null = $ps.AddParameter('Destination', $destination)
            $null = $ps.AddParameter('Confirm', $false)
            $null = $ps.Invoke()

            $ps.HadErrors | Should -BeFalse
            $records = @($ps.Streams.Progress)
            $records.Count | Should -BeGreaterThan 1
            ($records | Where-Object RecordType -EQ 'Processing' | Measure-Object -Property PercentComplete -Maximum).Maximum | Should -Be 100
            $records[-1].RecordType | Should -Be 'Completed'
            Get-BackupTestRowCount -Path $destination | Should -Be 512
        } finally {
            $ps.Dispose()
            Remove-BackupTestDatabase -Path $source, $destination
        }
    }

    It 'stops a <Method> backup waiting on a locked database and removes the destination' -TestCases @(
        @{ Method = 'Auto' }
        @{ Method = 'Snapshot' }
    ) {
        param($Method)

        $source = New-BackupTestDatabase -JournalMode DELETE
        $destination = "$source.backup"
        $connectionType = Get-SqliteProviderType -Name 'Microsoft.Data.Sqlite.SqliteConnection'
        $lock = [Activator]::CreateInstance($connectionType, [object[]] @("Data Source=$source;Pooling=False"))
        $ps = New-ModuleRunspacePowerShell
        try {
            # An exclusive lock in rollback-journal mode keeps every reader out, so the backup retries until stopped.
            $lock.Open()
            $command = $lock.CreateCommand()
            $command.CommandText = 'BEGIN EXCLUSIVE;'
            $null = $command.ExecuteNonQuery()

            $null = $ps.AddCommand('Invoke-DbaXSQLiteMaintenance')
            $null = $ps.AddParameter('Database', $source)
            $null = $ps.AddParameter('Action', 'Backup')
            $null = $ps.AddParameter('Destination', $destination)
            $null = $ps.AddParameter('BackupMethod', $Method)
            $null = $ps.AddParameter('BusyTimeoutMs', 120000)
            $null = $ps.AddParameter('Confirm', $false)
            $invocation = $ps.BeginInvoke()
            # The backup has started once it opened its destination; stopping earlier would not test cancellation.
            $deadline = [DateTime]::UtcNow.AddSeconds(30)
            while (-not (Test-Path -LiteralPath $destination) -and -not $invocation.IsCompleted -and [DateTime]::UtcNow -lt $deadline) {
                Start-Sleep -Milliseconds 50
            }
            Test-Path -LiteralPath $destination | Should -BeTrue
            Start-Sleep -Milliseconds 300
            $ps.InvocationStateInfo.State | Should -Be 'Running'

            $stopwatch = [Diagnostics.Stopwatch]::StartNew()
            $ps.Stop()
            $stopwatch.Stop()

            $stopwatch.Elapsed.TotalSeconds | Should -BeLessThan 10
            $ps.InvocationStateInfo.State | Should -Be 'Stopped'
            $invocation.IsCompleted | Should -BeTrue
            # Stopping returns once the pipeline stops; the canceled backup thread removes its partial copy right after.
            $deadline = [DateTime]::UtcNow.AddSeconds(10)
            while ((Test-Path -LiteralPath $destination) -and [DateTime]::UtcNow -lt $deadline) {
                Start-Sleep -Milliseconds 50
            }
            Test-Path -LiteralPath $destination | Should -BeFalse
        } finally {
            $ps.Dispose()
            $lock.Dispose()
            Remove-BackupTestDatabase -Path $source, $destination
        }
    }
}
