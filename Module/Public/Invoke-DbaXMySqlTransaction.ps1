function Invoke-DbaXMySqlTransaction {
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

    if (-not $PSCmdlet.ShouldProcess("$Server/$Database", 'Execute MySQL transaction')) {
        return
    }

    $clientFactory = Get-DbaXTransactionClientFactory -ProviderName 'MySql' -DefaultFactory { [DBAClientX.MySql]::new() }
    $credentialInfo = Resolve-DbaXExplicitTransactionCredential -Username $Username -Password $Password -Credential $Credential -ProviderName 'MySQL'
    if (-not (Test-DbaXTransactionClientFactoryOverride -ProviderName 'MySql')) {
        $connectionString = [DBAClientX.MySql]::BuildConnectionString($Server, $Database, $credentialInfo.Username, $credentialInfo.Password)
        $errorAction = Resolve-DbaXTransactionErrorAction -BoundParameters $PSBoundParameters
        if (-not (Test-DbaXTransactionConnection -PSCmdlet $PSCmdlet -ProviderAlias 'mysql' -ConnectionString $connectionString -ResolvedErrorAction $errorAction)) {
            return
        }
    }

    Invoke-DbaXTransactionInternal `
        -ProviderName 'MySQL' `
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
