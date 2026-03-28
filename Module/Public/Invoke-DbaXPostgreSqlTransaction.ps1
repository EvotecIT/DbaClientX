function Invoke-DbaXPostgreSqlTransaction {
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

    if (-not $PSCmdlet.ShouldProcess("$Server/$Database", 'Execute PostgreSQL transaction')) {
        return
    }

    $clientFactory = Get-DbaXTransactionClientFactory -ProviderName 'PostgreSql' -DefaultFactory { [DBAClientX.PostgreSql]::new() }
    $credentialInfo = Resolve-DbaXExplicitTransactionCredential -Username $Username -Password $Password -Credential $Credential -ProviderName 'PostgreSQL'
    if (-not (Test-DbaXTransactionClientFactoryOverride -ProviderName 'PostgreSql')) {
        $connectionString = [DBAClientX.PostgreSql]::BuildConnectionString($Server, $Database, $credentialInfo.Username, $credentialInfo.Password)
        $errorAction = Resolve-DbaXTransactionErrorAction -BoundParameters $PSBoundParameters
        if (-not (Test-DbaXTransactionConnection -PSCmdlet $PSCmdlet -ProviderAlias 'postgresql' -ConnectionString $connectionString -ResolvedErrorAction $errorAction)) {
            return
        }
    }

    Invoke-DbaXTransactionInternal `
        -ProviderName 'PostgreSQL' `
        -ClientFactory $clientFactory `
        -BeginTransaction {
            param($client)
            $client.BeginTransaction(
                $Server,
                $Database,
                $credentialInfo.Username,
                $credentialInfo.Password,
                $IsolationLevel)
        } `
        -ScriptBlock $ScriptBlock `
        -ArgumentList $ArgumentList `
        -QueryTimeout $QueryTimeout `
        -ApplyQueryTimeout:$PSBoundParameters.ContainsKey('QueryTimeout')
}
