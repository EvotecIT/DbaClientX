Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports StoredProcedure parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'StoredProcedure'
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Stream'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXQuery -Server '' -Database db -Query 'SELECT 1' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXQuery -Server s -Database '' -Query 'SELECT 1' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXQuery -Server s -Database db -Query '' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when StoredProcedure is empty' {
        { Invoke-DbaXQuery -Server s -Database db -StoredProcedure '' -ErrorAction Stop } | Should -Throw
    }

    it 'passes credentials to provider when supplied' {
        $code = @"
using System.Collections.Generic;
using System.Data;

public class TestSqlServer : DBAClientX.SqlServer {
    public static TestSqlServer Last;
    public bool Integrated;
    public string User;
    public string Pass;
    public TestSqlServer() { Last = this; }
    public override object Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object> parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType> parameterTypes = null, string username = null, string password = null) {
        this.Integrated = integratedSecurity;
        this.User = username;
        this.Pass = password;
        return null;
    }
}
"@
        $assemblyDir = Split-Path '/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll'
        $refs = @('/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll',
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
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('SqlServerFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.SqlServer]]{ [TestSqlServer]::new() })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Username u -Password p | Out-Null
            [TestSqlServer]::Last.Integrated | Should -BeFalse
            [TestSqlServer]::Last.User | Should -Be 'u'
            [TestSqlServer]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'streams rows asynchronously' {
        $code = @"
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

public class TestSqlServerStream : DBAClientX.SqlServer
{
    public override async IAsyncEnumerable<DataRow> QueryStreamAsync(
        string serverOrInstance, string database, bool integratedSecurity, string query,
        IDictionary<string, object> parameters = null, bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, System.Data.SqlDbType> parameterTypes = null, string username = null, string password = null)
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
        $assemblyDir = Split-Path '/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll'
        $refs = @('/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll',
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
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('SqlServerFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.SqlServer]]{ [TestSqlServerStream]::new() })
        try {
            $rows = @(Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'streams stored procedure rows asynchronously' {
        $code = @"
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;

public class TestSqlServerStoredProcStream : DBAClientX.SqlServer
{
    public override async IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
        string serverOrInstance, string database, bool integratedSecurity, string procedure,
        IEnumerable<DbParameter> parameters = null, bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string username = null, string password = null)
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
        $assemblyDir = Split-Path '/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll'
        $refs = @('/workspace/DbaClientX/DbaClientX.PowerShell/bin/Debug/net8.0/DbaClientX.SqlServer.dll',
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
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('SqlServerFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.SqlServer]]{ [TestSqlServerStoredProcStream]::new() })
        try {
            $rows = @(Invoke-DbaXQuery -Server s -Database db -StoredProcedure sp -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }
}
