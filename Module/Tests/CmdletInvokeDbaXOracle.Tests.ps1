Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracle cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracle | Should -Not -BeNullOrEmpty
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Stream'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'ReturnType'
    }

    it 'streams rows asynchronously' {
        $code = @"
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

public class TestOracleStream : DBAClientX.Oracle
{
    public override async IAsyncEnumerable<DataRow> QueryStreamAsync(
        string host, string serviceName, string username, string password, string query,
        IDictionary<string, object?>? parameters = null, bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, Oracle.ManagedDataAccess.Client.OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        for (int i = 1; i <= 2; i++)
        {
            var row = table.NewRow();
            row["id"] = i;
            table.Rows.Add(row);
        }
        for (int i = 0; i < table.Rows.Count; i++)
        {
            await Task.Yield();
            yield return table.Rows[i];
        }
    }
}
"@
        $assemblyDir = Split-Path '/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.Oracle.dll'
        $refs = @('/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.Oracle.dll',
                  (Join-Path $assemblyDir 'DbaClientX.Core.dll'),
                  [System.Data.DataTable].Assembly.Location,
                  [object].Assembly.Location,
                  [System.Runtime.GCSettings].Assembly.Location)
        try {
            Add-Type -TypeDefinition $code -ReferencedAssemblies $refs -CompilerOptions '/langversion:latest'
        } catch {
            Set-ItResult -Skipped -Because $_.Exception.Message
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracle].GetProperty('OracleFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.Oracle]]{ [TestOracleStream]::new() })
        try {
            $rows = @(Invoke-DbaXOracle -Server s -Database db -Username u -Password p -Query 'SELECT 1' -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }
}
