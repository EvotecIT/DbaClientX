function Invoke-DbaXOracleTransaction {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [Alias('DBServer', 'SqlInstance', 'Instance')]
        [ValidateNotNullOrEmpty()]
        [string] $Server,

        [Parameter(Mandatory)]
        [Alias('ServiceName')]
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

    if (-not $PSCmdlet.ShouldProcess("$Server/$Database", 'Execute Oracle transaction')) {
        return
    }

    $clientFactory = Get-DbaXTransactionClientFactory -ProviderName 'Oracle' -DefaultFactory { [DBAClientX.Oracle]::new() }
    $credentialInfo = Resolve-DbaXExplicitTransactionCredential -Username $Username -Password $Password -Credential $Credential -ProviderName 'Oracle'
    if (-not (Test-DbaXTransactionClientFactoryOverride -ProviderName 'Oracle')) {
        $connectionString = [DBAClientX.Oracle]::BuildConnectionString($Server, $Database, $credentialInfo.Username, $credentialInfo.Password)
        $errorAction = Resolve-DbaXTransactionErrorAction -BoundParameters $PSBoundParameters
        if (-not (Test-DbaXTransactionConnection -PSCmdlet $PSCmdlet -ProviderAlias 'oracle' -ConnectionString $connectionString -ResolvedErrorAction $errorAction)) {
            return
        }
    }

    Invoke-DbaXTransactionInternal `
        -ProviderName 'Oracle' `
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
