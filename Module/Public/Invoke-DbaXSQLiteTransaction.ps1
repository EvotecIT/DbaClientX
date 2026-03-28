function Invoke-DbaXSQLiteTransaction {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string] $Database,

        [Parameter(Mandatory)]
        [ValidateNotNull()]
        [scriptblock] $ScriptBlock,

        [Parameter()]
        [int] $QueryTimeout,

        [Parameter()]
        [System.Data.IsolationLevel] $IsolationLevel = [System.Data.IsolationLevel]::ReadCommitted,

        [Parameter()]
        [object[]] $ArgumentList
    )

    if (-not $PSCmdlet.ShouldProcess($Database, 'Execute SQLite transaction')) {
        return
    }

    $clientFactory = Get-DbaXTransactionClientFactory -ProviderName 'SQLite' -DefaultFactory { [DBAClientX.SQLite]::new() }
    if (-not (Test-DbaXTransactionClientFactoryOverride -ProviderName 'SQLite')) {
        $connectionString = [DBAClientX.SQLite]::BuildConnectionString($Database)
        $errorAction = Resolve-DbaXTransactionErrorAction -BoundParameters $PSBoundParameters
        if (-not (Test-DbaXTransactionConnection -PSCmdlet $PSCmdlet -ProviderAlias 'sqlite' -ConnectionString $connectionString -ResolvedErrorAction $errorAction)) {
            return
        }
    }

    Invoke-DbaXTransactionInternal `
        -ProviderName 'SQLite' `
        -ClientFactory $clientFactory `
        -BeginTransaction {
            param($client)
            $client.BeginTransaction($Database, $IsolationLevel)
        } `
        -ScriptBlock $ScriptBlock `
        -ArgumentList $ArgumentList `
        -QueryTimeout $QueryTimeout `
        -ApplyQueryTimeout:$PSBoundParameters.ContainsKey('QueryTimeout')
}
