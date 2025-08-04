Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXQuery streaming' {
    BeforeAll {
        Add-Type -ReferencedAssemblies @('System.Data', 'System.Data.Common', 'System.ComponentModel.TypeConverter') -TypeDefinition @"
#nullable enable
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

public static class DummyStream
{
    public static async IAsyncEnumerable<DataRow> GetRows()
    {
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        var r1 = table.NewRow();
        r1["id"] = 1;
        table.Rows.Add(r1);
        var r2 = table.NewRow();
        r2["id"] = 2;
        table.Rows.Add(r2);
        foreach (DataRow row in table.Rows)
        {
            await Task.Yield();
            yield return row;
        }
    }
}
"@
        $script:oldStream = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::StreamGenerator
        [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::StreamGenerator = { [DummyStream]::GetRows() }
    }

    AfterAll {
        [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::StreamGenerator = $script:oldStream
    }

    It 'emits streamed rows as PSObjects' {
        $rows = Invoke-DbaXQuery -Server 's' -Database 'd' -Query 'q' -Stream
        $rows.Count | Should -Be 2
        $rows[0].id | Should -Be 1
        $rows[0] | Should -BeOfType [pscustomobject]
    }
}


