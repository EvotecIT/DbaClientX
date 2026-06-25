Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

if (-not ('DbaXTestDataRecord' -as [type])) {
    $referencedAssemblies = if ($PSVersionTable.PSEdition -eq 'Core') { @('System.Data.Common') } else { @('System.Data') }
    Add-Type -ReferencedAssemblies $referencedAssemblies -TypeDefinition @'
using System;
using System.Data;

public sealed class DbaXTestDataRecord : IDataRecord
{
    private readonly string[] _names;
    private readonly Type[] _types;
    private readonly object[] _values;

    public DbaXTestDataRecord(string[] names, object[] values)
    {
        _names = names;
        _values = values;
        _types = new Type[values.Length];
        for (int index = 0; index < values.Length; index++)
        {
            _types[index] = values[index] == null || values[index] == DBNull.Value
                ? typeof(object)
                : values[index].GetType();
        }
    }

    public object this[int i] { get { return GetValue(i); } }

    public object this[string name] { get { return GetValue(GetOrdinal(name)); } }

    public int FieldCount { get { return _values.Length; } }

    public bool GetBoolean(int i) { return Convert.ToBoolean(GetValue(i)); }

    public byte GetByte(int i) { return Convert.ToByte(GetValue(i)); }

    public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotSupportedException(); }

    public char GetChar(int i) { return Convert.ToChar(GetValue(i)); }

    public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotSupportedException(); }

    public IDataReader GetData(int i) { throw new NotSupportedException(); }

    public string GetDataTypeName(int i) { return GetFieldType(i).Name; }

    public DateTime GetDateTime(int i) { return Convert.ToDateTime(GetValue(i)); }

    public decimal GetDecimal(int i) { return Convert.ToDecimal(GetValue(i)); }

    public double GetDouble(int i) { return Convert.ToDouble(GetValue(i)); }

    public Type GetFieldType(int i) { return _types[i]; }

    public float GetFloat(int i) { return Convert.ToSingle(GetValue(i)); }

    public Guid GetGuid(int i) { return (Guid)GetValue(i); }

    public short GetInt16(int i) { return Convert.ToInt16(GetValue(i)); }

    public int GetInt32(int i) { return Convert.ToInt32(GetValue(i)); }

    public long GetInt64(int i) { return Convert.ToInt64(GetValue(i)); }

    public string GetName(int i) { return _names[i]; }

    public int GetOrdinal(string name)
    {
        for (int index = 0; index < _names.Length; index++)
        {
            if (string.Equals(_names[index], name, StringComparison.OrdinalIgnoreCase))
            {
                return index;
            }
        }

        return -1;
    }

    public string GetString(int i) { return Convert.ToString(GetValue(i)); }

    public object GetValue(int i) { return _values[i]; }

    public int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, _values.Length);
        Array.Copy(_values, values, count);
        return count;
    }

    public bool IsDBNull(int i)
    {
        return _values[i] == null || _values[i] == DBNull.Value;
    }
}
'@
}

describe 'Write-DbaXTableData cmdlet' {
    it 'is exported' {
        Get-Command Write-DbaXTableData | Should -Not -BeNullOrEmpty
    }

    it 'supports provider routing parameters' {
        $parameters = (Get-Command Write-DbaXTableData).Parameters.Keys
        $parameters | Should -Contain 'Provider'
        $parameters | Should -Contain 'ConnectionString'
        $parameters | Should -Contain 'DestinationTable'
        $parameters | Should -Contain 'BatchSize'
        $parameters | Should -Contain 'BulkCopyTimeout'
        $parameters | Should -Contain 'PassThru'
    }

    it 'passes DataTable input to the provider bulk layer' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkInsert = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkInsert = [pscustomobject]@{
                Provider = $cmdlet.Provider.ToString()
                DestinationTable = $cmdlet.DestinationTable
                BatchSize = $cmdlet.BatchSize
                BulkCopyTimeout = $cmdlet.BulkCopyTimeout
                Rows = $table.Rows.Count
                FirstValue = $table.Rows[0]['Name']
            }
        })

        try {
            $table = [System.Data.DataTable]::new('Input')
            $null = $table.Columns.Add('Name', [string])
            $row = $table.NewRow()
            $row['Name'] = 'Alpha'
            $table.Rows.Add($row)

            $result = $table | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -BatchSize 100 -BulkCopyTimeout 30 -PassThru

            $script:lastBulkInsert.Provider | Should -Be 'SqlServer'
            $script:lastBulkInsert.DestinationTable | Should -Be 'dbo.Import'
            $script:lastBulkInsert.BatchSize | Should -Be 100
            $script:lastBulkInsert.BulkCopyTimeout | Should -Be 30
            $script:lastBulkInsert.Rows | Should -Be 1
            $script:lastBulkInsert.FirstValue | Should -Be 'Alpha'
            $result.Rows | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkInsert = $null
        }
    }

    it 'converts object pipeline input to a DataTable' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            [pscustomobject]@{ Id = 1; Name = 'Alpha' },
            [pscustomobject]@{ Id = 2; Name = 'Beta' } |
                Write-DbaXTableData -Provider SQLite -ConnectionString 'Data Source=C:\Temp\bulk-test.db' -DestinationTable Import | Out-Null

            $script:lastBulkTable.Rows.Count | Should -Be 2
            $script:lastBulkTable.Columns['Id'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Columns['Name'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[0]['Id'].GetType().FullName | Should -Be 'System.Int32'
            $script:lastBulkTable.Rows[1]['Name'] | Should -Be 'Beta'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'treats an empty pipeline as a no-op' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:bulkCalls = 0
        $prop.SetValue($null, [scriptblock]{
            $script:bulkCalls++
        })

        try {
            $table = [System.Data.DataTable]::new('Input')
            $null = $table.Columns.Add('Name', [string])

            $result = $table | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -PassThru

            $script:bulkCalls | Should -Be 0
            $result.Rows | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:bulkCalls = 0
        }
    }

    it 'merges compatible DataRow input from different tables' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            $first = [System.Data.DataTable]::new('First')
            $null = $first.Columns.Add('Name', [string])
            $firstRow = $first.NewRow()
            $firstRow['Name'] = 'Alpha'
            $first.Rows.Add($firstRow)

            $second = [System.Data.DataTable]::new('Second')
            $null = $second.Columns.Add('Name', [string])
            $secondRow = $second.NewRow()
            $secondRow['Name'] = 'Beta'
            $second.Rows.Add($secondRow)

            @($first.Rows[0], $second.Rows[0]) |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Rows.Count | Should -Be 2
            $script:lastBulkTable.Rows[0]['Name'] | Should -Be 'Alpha'
            $script:lastBulkTable.Rows[1]['Name'] | Should -Be 'Beta'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'converts DataRowView pipeline input as table rows' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            $table = [System.Data.DataTable]::new('Input')
            $null = $table.Columns.Add('Name', [string])
            $row = $table.NewRow()
            $row['Name'] = 'Alpha'
            $table.Rows.Add($row)

            $view = [System.Data.DataView]::new($table)
            $view | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Rows.Count | Should -Be 1
            $script:lastBulkTable.Columns['Name'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[0]['Name'] | Should -Be 'Alpha'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'converts IDataRecord pipeline input as table rows' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            $table = [System.Data.DataTable]::new('Input')
            $null = $table.Columns.Add('Name', [string])
            $row = $table.NewRow()
            $row['Name'] = 'Alpha'
            $table.Rows.Add($row)

            $reader = $table.CreateDataReader()
            $reader | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Rows.Count | Should -Be 1
            $script:lastBulkTable.Columns['Name'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[0]['Name'] | Should -Be 'Alpha'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'keeps duplicate IDataRecord field names unique' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            $record = [DbaXTestDataRecord]::new(
                [string[]]@('Id', 'Id'),
                [object[]]@(1, 2))

            $record | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Columns['Id'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Columns['Id_2'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[0]['Id'] | Should -Be 1
            $script:lastBulkTable.Rows[0]['Id_2'] | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'rejects incompatible IDataRecord schemas' {
        $first = [DbaXTestDataRecord]::new(
            [string[]]@('Id', 'Name'),
            [object[]]@(1, 'Alpha'))
        $second = [DbaXTestDataRecord]::new(
            [string[]]@('Id', 'DisplayName'),
            [object[]]@(2, 'Beta'))

        {
            @($first, $second) |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -ErrorAction Stop
        } | Should -Throw
    }

    it 'preserves scalar pipeline input as values' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            'Alpha', 'Beta' |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Columns['Value'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[0]['Value'] | Should -Be 'Alpha'
            $script:lastBulkTable.Rows[1]['Value'] | Should -Be 'Beta'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'preserves explicit null pipeline input as values' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            $null, 'Beta' |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import | Out-Null

            $script:lastBulkTable.Columns['Value'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows.Count | Should -Be 2
            $script:lastBulkTable.Rows[0]['Value'] | Should -Be ([DBNull]::Value)
            $script:lastBulkTable.Rows[1]['Value'] | Should -Be 'Beta'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:bulkCalls = 0
        $prop.SetValue($null, [scriptblock]{
            $script:bulkCalls++
        })

        try {
            [pscustomobject]@{ Id = 1 } |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -WhatIf | Out-Null
            $script:bulkCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:bulkCalls = 0
        }
    }

    it 'rejects SQLite bulk copy timeout' {
        {
            [pscustomobject]@{ Id = 1 } |
                Write-DbaXTableData -Provider SQLite -ConnectionString 'Data Source=C:\Temp\bulk-test.db' -DestinationTable Import -BulkCopyTimeout 30 -ErrorAction Stop
        } | Should -Throw
    }
}
