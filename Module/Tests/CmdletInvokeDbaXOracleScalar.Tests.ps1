Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracleScalar cmdlet' {
    BeforeAll {
        . "$PSScriptRoot/TestAssemblyHelpers.ps1"
    }

    it 'is exported' {
        Get-Command Invoke-DbaXOracleScalar | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'ReturnType'
    }

    it 'passes credentials to provider when supplied' {
        $code = @"
#nullable enable
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

public class TestOracleScalarCredentials : DBAClientX.Oracle
{
    public static TestOracleScalarCredentials Last;
    public string? User;
    public string? Pass;

    public TestOracleScalarCredentials()
    {
        Last = this;
    }

    public override Task<object?> ExecuteScalarAsync(
        string host, string serviceName, string username, string password, string query,
        IDictionary<string, object?>? parameters = null, bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, Oracle.ManagedDataAccess.Client.OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        User = username;
        Pass = password;
        return Task.FromResult<object?>(1);
    }
}
"@
        $refs = Get-TestAssemblyReferences -ProviderType ([DBAClientX.Oracle]) -AdditionalAssemblyPaths ([Oracle.ManagedDataAccess.Client.OracleDbType].Assembly.Location)
        try {
            Add-Type -TypeDefinition $code -ReferencedAssemblies $refs -CompilerOptions @('/langversion:latest', '/nullable:enable') -IgnoreWarnings
        } catch {
            Set-ItResult -Skipped -Because $_.Exception.Message
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('OracleFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.Oracle]]{ [TestOracleScalarCredentials]::new() })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p | Out-Null
            [TestOracleScalarCredentials]::Last.User | Should -Be 'u'
            [TestOracleScalarCredentials]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'passes QueryTimeout and Parameters to provider' {
        $code = @"
#nullable enable
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

public class TestOracleScalarOptions : DBAClientX.Oracle
{
    public static TestOracleScalarOptions Last;
    public int Timeout;
    public IDictionary<string, object?>? Params;

    public TestOracleScalarOptions()
    {
        Last = this;
    }

    public override Task<object?> ExecuteScalarAsync(
        string host, string serviceName, string username, string password, string query,
        IDictionary<string, object?>? parameters = null, bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, Oracle.ManagedDataAccess.Client.OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        Timeout = CommandTimeout;
        Params = parameters;
        return Task.FromResult<object?>(1);
    }
}
"@
        $refs = Get-TestAssemblyReferences -ProviderType ([DBAClientX.Oracle]) -AdditionalAssemblyPaths ([Oracle.ManagedDataAccess.Client.OracleDbType].Assembly.Location)
        try {
            Add-Type -TypeDefinition $code -ReferencedAssemblies $refs -CompilerOptions @('/langversion:latest', '/nullable:enable') -IgnoreWarnings
        } catch {
            Set-ItResult -Skipped -Because $_.Exception.Message
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('OracleFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.Oracle]]{ [TestOracleScalarOptions]::new() })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p -QueryTimeout 7 -Parameters @{ A = 1 } | Out-Null
            [TestOracleScalarOptions]::Last.Timeout | Should -Be 7
            [TestOracleScalarOptions]::Last.Params['A'] | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXOracleScalar -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
