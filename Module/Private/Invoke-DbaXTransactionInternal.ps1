function Get-DbaXTransactionClientFactoryOverrides {
    [CmdletBinding()]
    param()

    if (-not (Get-Variable -Scope Global -Name DbaXTransactionClientFactoryOverrides -ErrorAction SilentlyContinue)) {
        $global:DbaXTransactionClientFactoryOverrides = @{}
    }

    $global:DbaXTransactionClientFactoryOverrides
}

function Get-DbaXTransactionClientFactory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $ProviderName,

        [Parameter(Mandatory)]
        [scriptblock] $DefaultFactory
    )

    $overrides = Get-DbaXTransactionClientFactoryOverrides
    if ($overrides.Contains($ProviderName)) {
        return $overrides[$ProviderName]
    }

    $DefaultFactory
}

function Test-DbaXTransactionClientFactoryOverride {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $ProviderName
    )

    $overrides = Get-DbaXTransactionClientFactoryOverrides
    return $overrides.Contains($ProviderName)
}

function Resolve-DbaXSqlServerTransactionCredential {
    [CmdletBinding()]
    param(
        [string] $Username,
        [string] $Password,
        [System.Management.Automation.PSCredential] $Credential
    )

    if ($null -ne $Credential) {
        $networkCredential = $Credential.GetNetworkCredential()
        return @{
            Username = $networkCredential.UserName
            Password = $networkCredential.Password
            IntegratedSecurity = $false
        }
    }

    return @{
        Username = $Username
        Password = $Password
        IntegratedSecurity = [string]::IsNullOrEmpty($Username) -and [string]::IsNullOrEmpty($Password)
    }
}

function Resolve-DbaXExplicitTransactionCredential {
    [CmdletBinding()]
    param(
        [string] $Username,
        [string] $Password,
        [System.Management.Automation.PSCredential] $Credential,
        [Parameter(Mandatory)]
        [string] $ProviderName
    )

    if ($null -ne $Credential) {
        $networkCredential = $Credential.GetNetworkCredential()
        return @{
            Username = $networkCredential.UserName
            Password = $networkCredential.Password
        }
    }

    if ([string]::IsNullOrEmpty($Username) -or [string]::IsNullOrEmpty($Password)) {
        throw [System.Management.Automation.PSArgumentException]::new("Provide either -Credential or both -Username and -Password for $ProviderName authentication.")
    }

    return @{
        Username = $Username
        Password = $Password
    }
}

function Resolve-DbaXTransactionErrorAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [hashtable] $BoundParameters
    )

    $preference = $ErrorActionPreference
    if ($BoundParameters.ContainsKey('ErrorAction')) {
        $preference = [System.Management.Automation.ActionPreference] $BoundParameters['ErrorAction']
    }

    $preference
}

function Test-DbaXTransactionConnection {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [System.Management.Automation.PSCmdlet] $PSCmdlet,

        [Parameter(Mandatory)]
        [string] $ProviderAlias,

        [Parameter(Mandatory)]
        [string] $ConnectionString,

        [Parameter(Mandatory)]
        [System.Management.Automation.ActionPreference] $ResolvedErrorAction
    )

    $validation = [DBAClientX.Invoker.DbaConnectionFactory]::Validate($ProviderAlias, $ConnectionString)
    if ($validation.IsValid) {
        return $true
    }

    $message = [DBAClientX.Invoker.DbaConnectionFactory]::ToUserMessage($validation)
    if ($ResolvedErrorAction -eq [System.Management.Automation.ActionPreference]::Stop) {
        throw [System.Management.Automation.PSArgumentException]::new($message)
    }

    $PSCmdlet.WriteWarning($message)
    return $false
}

function Set-DbaXTransactionClientTimeout {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object] $Client,

        [Parameter(Mandatory)]
        [int] $QueryTimeout
    )

    $timeoutProperty = $Client.PSObject.Properties['CommandTimeout']
    if ($null -ne $timeoutProperty) {
        $timeoutProperty.Value = $QueryTimeout
    }
}

function Test-DbaXClientInTransaction {
    [CmdletBinding()]
    param(
        [object] $Client
    )

    if ($null -eq $Client) {
        return $false
    }

    $transactionProperty = $Client.PSObject.Properties['IsInTransaction']
    if ($null -eq $transactionProperty) {
        return $false
    }

    return [bool] $transactionProperty.Value
}

function Dispose-DbaXTransactionClient {
    [CmdletBinding()]
    param(
        [object] $Client
    )

    if ($null -eq $Client) {
        return
    }

    $disposeAsyncMethod = $Client.PSObject.Methods['DisposeAsync']
    if ($null -ne $disposeAsyncMethod) {
        $disposeTask = $Client.DisposeAsync()
        if ($null -ne $disposeTask) {
            $awaiter = $disposeTask.GetAwaiter()
            if ($null -ne $awaiter) {
                $awaiter.GetResult()
                return
            }
        }
    }

    if ($Client -is [System.IDisposable]) {
        $Client.Dispose()
        return
    }

    $disposeMethod = $Client.PSObject.Methods['Dispose']
    if ($null -ne $disposeMethod) {
        $Client.Dispose()
    }
}

function Invoke-DbaXTransactionInternal {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $ProviderName,

        [Parameter(Mandatory)]
        [scriptblock] $ClientFactory,

        [Parameter(Mandatory)]
        [scriptblock] $BeginTransaction,

        [Parameter(Mandatory)]
        [scriptblock] $ScriptBlock,

        [object[]] $ArgumentList,

        [int] $QueryTimeout,

        [switch] $ApplyQueryTimeout
    )

    $client = $null
    try {
        $client = & $ClientFactory
        if ($ApplyQueryTimeout) {
            Set-DbaXTransactionClientTimeout -Client $client -QueryTimeout $QueryTimeout
        }

        & $BeginTransaction $client
        $result = & $ScriptBlock $client @ArgumentList
        $client.Commit()
        $result
    } catch {
        $rollbackError = $null
        if (Test-DbaXClientInTransaction -Client $client) {
            try {
                $client.Rollback()
            } catch {
                $rollbackError = $_.Exception
            }
        }

        if ($null -ne $rollbackError) {
            throw [System.AggregateException]::new(
                "Transaction failed and rollback also failed for $ProviderName.",
                @($_.Exception, $rollbackError))
        }

        throw
    } finally {
        Dispose-DbaXTransactionClient -Client $client
    }
}
