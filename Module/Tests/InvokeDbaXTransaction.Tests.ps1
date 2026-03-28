Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

function Global:New-TransactionTestClient {
    param(
        [Parameter(Mandatory)]
        [hashtable] $State
    )

    $client = [pscustomobject]@{
        CommandTimeout = 0
        IsInTransaction = $false
        State = $State
    }

    $client | Add-Member -MemberType ScriptMethod -Name BeginTransaction -Value {
        $this.State.BeginCalls++
        $this.State.BeginArguments = @($args)
        if ($this.State.ThrowOnBegin) {
            throw [System.InvalidOperationException]::new('begin failed')
        }

        $this.IsInTransaction = $true
    }

    $client | Add-Member -MemberType ScriptMethod -Name Commit -Value {
        $this.State.CommitCalls++
        if ($this.State.ThrowOnCommit) {
            throw [System.InvalidOperationException]::new('commit failed')
        }

        $this.IsInTransaction = $false
    }

    $client | Add-Member -MemberType ScriptMethod -Name Rollback -Value {
        $this.State.RollbackCalls++
        if ($this.State.ThrowOnRollback) {
            throw [System.InvalidOperationException]::new('rollback failed')
        }

        $this.IsInTransaction = $false
    }

    $client | Add-Member -MemberType ScriptMethod -Name Dispose -Value {
        $this.State.DisposeCalls++
    }

    return $client
}

Describe 'Invoke-DbaX*Transaction functions' {
    BeforeEach {
        $global:DbaXTransactionClientFactoryOverrides = @{}
    }

    AfterEach {
        $global:DbaXTransactionClientFactoryOverrides = @{}
    }

    It 'exports transaction helper functions' {
        Get-Command Invoke-DbaXTransaction | Should -Not -BeNullOrEmpty
        Get-Command Invoke-DbaXMySqlTransaction | Should -Not -BeNullOrEmpty
        Get-Command Invoke-DbaXPostgreSqlTransaction | Should -Not -BeNullOrEmpty
        Get-Command Invoke-DbaXOracleTransaction | Should -Not -BeNullOrEmpty
        Get-Command Invoke-DbaXSQLiteTransaction | Should -Not -BeNullOrEmpty
    }

    It 'commits and disposes SQL Server transactions on success' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $global:DbaXTransactionClientFactoryOverrides['SqlServer'] = {
            New-TransactionTestClient -State $state
        }

        $result = Invoke-DbaXTransaction -Server 'sql' -Database 'app' -QueryTimeout 15 -ArgumentList 'value' -ScriptBlock {
            param($client, $item)
            $client.CommandTimeout | Should -Be 15
            $item | Should -Be 'value'
            'done'
        }

        $result | Should -Be 'done'
        $state.BeginCalls | Should -Be 1
        $state.CommitCalls | Should -Be 1
        $state.RollbackCalls | Should -Be 0
        $state.DisposeCalls | Should -Be 1
        $state.BeginArguments[0] | Should -Be 'sql'
        $state.BeginArguments[1] | Should -Be 'app'
        $state.BeginArguments[2] | Should -BeTrue
        $state.BeginArguments[3] | Should -Be ([System.Data.IsolationLevel]::ReadCommitted)
    }

    It 'rolls back and disposes SQL Server transactions on failure' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $global:DbaXTransactionClientFactoryOverrides['SqlServer'] = {
            New-TransactionTestClient -State $state
        }

        { Invoke-DbaXTransaction -Server 'sql' -Database 'app' -ScriptBlock { throw 'boom' } -ErrorAction Stop } | Should -Throw 'boom'
        $state.BeginCalls | Should -Be 1
        $state.CommitCalls | Should -Be 0
        $state.RollbackCalls | Should -Be 1
        $state.DisposeCalls | Should -Be 1
    }

    It 'surfaces rollback failures as aggregate exceptions' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
            ThrowOnRollback = $true
        }
        $global:DbaXTransactionClientFactoryOverrides['SqlServer'] = {
            New-TransactionTestClient -State $state
        }

        $failure = $null
        try {
            Invoke-DbaXTransaction -Server 'sql' -Database 'app' -ScriptBlock { throw [System.InvalidOperationException]::new('boom') } -ErrorAction Stop
        } catch {
            $failure = $_.Exception
        }

        $failure | Should -BeOfType ([System.AggregateException])
        $failure.InnerExceptions.Count | Should -Be 2
        $state.RollbackCalls | Should -Be 1
        $state.DisposeCalls | Should -Be 1
    }

    It 'honors WhatIf and skips client creation' {
        $script:factoryCalls = 0
        $global:DbaXTransactionClientFactoryOverrides['SqlServer'] = {
            $script:factoryCalls++
            New-TransactionTestClient -State @{
                BeginCalls = 0
                CommitCalls = 0
                RollbackCalls = 0
                DisposeCalls = 0
                BeginArguments = @()
            }
        }

        Invoke-DbaXTransaction -Server 'sql' -Database 'app' -ScriptBlock { 'unused' } -WhatIf | Out-Null
        $script:factoryCalls | Should -Be 0
    }

    It 'passes PSCredential values to explicit-auth providers' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $global:DbaXTransactionClientFactoryOverrides['MySql'] = {
            New-TransactionTestClient -State $state
        }

        Invoke-DbaXMySqlTransaction -Server 'mysql' -Database 'app' -Credential $credential -ScriptBlock { 'ok' } | Out-Null

        $state.BeginArguments[0] | Should -Be 'mysql'
        $state.BeginArguments[1] | Should -Be 'app'
        $state.BeginArguments[2] | Should -Be 'u'
        $state.BeginArguments[3] | Should -Be 'p'
        $state.CommitCalls | Should -Be 1
        $state.DisposeCalls | Should -Be 1
    }

    It 'uses explicit credentials for PostgreSQL transactions' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $global:DbaXTransactionClientFactoryOverrides['PostgreSql'] = {
            New-TransactionTestClient -State $state
        }

        Invoke-DbaXPostgreSqlTransaction -Server 'pg' -Database 'app' -Username 'user' -Password 'pass' -ScriptBlock { 'ok' } | Out-Null

        $state.BeginArguments[2] | Should -Be 'user'
        $state.BeginArguments[3] | Should -Be 'pass'
        $state.CommitCalls | Should -Be 1
    }

    It 'uses explicit credentials for Oracle transactions' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $global:DbaXTransactionClientFactoryOverrides['Oracle'] = {
            New-TransactionTestClient -State $state
        }

        Invoke-DbaXOracleTransaction -Server 'ora' -Database 'svc' -Username 'user' -Password 'pass' -ScriptBlock { 'ok' } | Out-Null

        $state.BeginArguments[0] | Should -Be 'ora'
        $state.BeginArguments[1] | Should -Be 'svc'
        $state.BeginArguments[2] | Should -Be 'user'
        $state.BeginArguments[3] | Should -Be 'pass'
        $state.CommitCalls | Should -Be 1
    }

    It 'commits and disposes SQLite transactions on success' {
        $state = @{
            BeginCalls = 0
            CommitCalls = 0
            RollbackCalls = 0
            DisposeCalls = 0
            BeginArguments = @()
        }
        $global:DbaXTransactionClientFactoryOverrides['SQLite'] = {
            New-TransactionTestClient -State $state
        }

        Invoke-DbaXSQLiteTransaction -Database 'app.db' -QueryTimeout 9 -ScriptBlock {
            param($client)
            $client.CommandTimeout | Should -Be 9
            'ok'
        } | Out-Null

        $state.BeginArguments[0] | Should -Be 'app.db'
        $state.BeginArguments[1] | Should -Be ([System.Data.IsolationLevel]::ReadCommitted)
        $state.CommitCalls | Should -Be 1
        $state.DisposeCalls | Should -Be 1
    }

    It 'warns for unsafe sqlite transaction paths' {
        $warnings = $null

        Invoke-DbaXSQLiteTransaction -Database '../unsafe.db' -ScriptBlock { 'unused' } -ErrorAction Continue -WarningVariable warnings | Out-Null

        @($warnings).Count | Should -BeGreaterThan 0
        ($warnings -join "`n") | Should -Match 'unsafe relative path'
    }

    It 'skips sqlite validation warnings under WhatIf' {
        $warnings = $null

        Invoke-DbaXSQLiteTransaction -Database '../unsafe.db' -ScriptBlock { 'unused' } -WhatIf -WarningVariable warnings | Out-Null

        @($warnings).Count | Should -Be 0
    }

    It 'throws on unsafe sqlite transaction paths when ErrorAction is Stop' {
        { Invoke-DbaXSQLiteTransaction -Database '../unsafe.db' -ScriptBlock { 'unused' } -ErrorAction Stop } | Should -Throw
    }
}
