function Invoke-DbaXTransaction {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [Alias('DBServer', 'SqlInstance', 'Instance')]
        [ValidateNotNullOrEmpty()]
        [string] $Server,

        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string] $Database,

        [Parameter(Mandatory)]
        [ValidateNotNull()]
        [scriptblock] $ScriptBlock,

        [Parameter()]
        [string] $Username = '',

        [Parameter()]
        [string] $Password = '',

        [Parameter()]
        [System.Management.Automation.PSCredential] $Credential,

        [Parameter()]
        [int] $QueryTimeout,

        [Parameter()]
        [System.Data.IsolationLevel] $IsolationLevel = [System.Data.IsolationLevel]::ReadCommitted,

        [Parameter()]
        [object[]] $ArgumentList
    )

    if (-not $PSCmdlet.ShouldProcess("$Server/$Database", 'Execute SQL Server transaction')) {
        return
    }

    $clientFactory = Get-DbaXTransactionClientFactory -ProviderName 'SqlServer' -DefaultFactory { [DBAClientX.SqlServer]::new() }
    $credentialInfo = Resolve-DbaXSqlServerTransactionCredential -Username $Username -Password $Password -Credential $Credential
    if (-not (Test-DbaXTransactionClientFactoryOverride -ProviderName 'SqlServer')) {
        $connectionString = [DBAClientX.SqlServer]::BuildConnectionString(
            $Server,
            $Database,
            $credentialInfo.IntegratedSecurity,
            $credentialInfo.Username,
            $credentialInfo.Password)
        $errorAction = Resolve-DbaXTransactionErrorAction -BoundParameters $PSBoundParameters
        if (-not (Test-DbaXTransactionConnection -PSCmdlet $PSCmdlet -ProviderAlias 'sqlserver' -ConnectionString $connectionString -ResolvedErrorAction $errorAction)) {
            return
        }
    }

    Invoke-DbaXTransactionInternal `
        -ProviderName 'SQL Server' `
        -ClientFactory $clientFactory `
        -BeginTransaction {
            param($client)
            $client.BeginTransaction(
                $Server,
                $Database,
                $credentialInfo.IntegratedSecurity,
                $IsolationLevel,
                $credentialInfo.Username,
                $credentialInfo.Password)
        } `
        -ScriptBlock $ScriptBlock `
        -ArgumentList $ArgumentList `
        -QueryTimeout $QueryTimeout `
        -ApplyQueryTimeout:$PSBoundParameters.ContainsKey('QueryTimeout')
}
