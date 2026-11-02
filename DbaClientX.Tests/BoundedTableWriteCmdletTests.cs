using System.Data;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public sealed class BoundedTableWriteCmdletTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "dbaclientx-batches-" + Guid.NewGuid().ToString("N") + ".db");

    public BoundedTableWriteCmdletTests()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(_path, "CREATE TABLE Records (Id INTEGER PRIMARY KEY, Name TEXT)");
    }

    [Fact]
    public void InputBatches_WriteBeforeUpstreamCompletes_AndFlushFinalRows()
    {
        using var ps = CreateShell();
        ps.AddScript("""
            1..5 | ForEach-Object {
                if ($_ -eq 3) {
                    $count = Invoke-DbaXSQLite -Database $db -Query 'SELECT COUNT(*) AS Rows FROM Records'
                    if ($count.Rows -ne 2) { throw 'The completed input batch was buffered.' }
                }
                if ($_ % 2) { [pscustomobject]@{ Id = $_; Name = "row$_" } }
                else { [pscustomobject]@{ Name = "row$_"; Id = $_ } }
            } | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 2 -BatchSize 1 -PassThru -ErrorAction Stop
            """);

        var result = Assert.Single(ps.Invoke());
        Assert.False(ps.HadErrors, string.Join("; ", ps.Streams.Error));
        Assert.Equal(5L, result.Properties["Rows"].Value);
        Assert.Equal(new[] { "row1", "row2", "row3", "row4", "row5" }, ReadNames());
    }

    [Theory]
    [InlineData("[pscustomobject]@{ Id = 3; Name = 'third'; Extra = 'must not disappear' }")]
    [InlineData("[pscustomobject]@{ Id = 'third'; Name = 'third' }")]
    public void InputBatches_RejectSchemaDrift_AndStopAfterError(string changedRow)
    {
        using var ps = CreateShell();
        ps.AddScript("""
            & {
                [pscustomobject]@{ Id = 1; Name = 'first' }
                [pscustomobject]@{ Id = 2; Name = 'second' }
            """ + "\n" + changedRow + "\n" + """

                [pscustomobject]@{ Id = 4; Name = 'must not be written' }
            } | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 1 -PassThru -ErrorAction Continue
            """);

        Assert.Empty(ps.Invoke());
        Assert.Contains("first batch", Assert.Single(ps.Streams.Error).Exception.Message, StringComparison.Ordinal);
        Assert.Equal(new[] { "first", "second" }, ReadNames());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void InputBatches_ProviderFailure_DoesNotWriteLaterBatches(bool stop)
    {
        using var ps = CreateShell();
        var script = """
            @(1, 2, 3, 1, 5, 6) | ForEach-Object {
                [pscustomobject]@{ Id = $_; Name = "row$_" }
            } | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 2 -PassThru -ErrorAction
            """ + (stop ? " Stop" : " Continue");
        ps.AddScript(stop ? "try {\n" + script + "\n} catch { 'caught' }" : script);

        var result = ps.Invoke();
        if (stop) Assert.Equal("caught", Assert.Single(result).BaseObject);
        else
        {
            Assert.Empty(result);
            Assert.Single(ps.Streams.Error);
        }
        Assert.Equal(new[] { "row1", "row2" }, ReadNames());
    }

    [Fact]
    public void InputBatches_NullBatch_PreservesFirstBatchTypes()
    {
        using var ps = CreateShell();
        ps.AddScript("""
            @(
                [pscustomobject]@{ Id = 1; Name = 'first' }
                [pscustomobject]@{ Name = $null; Id = 2 }
            ) | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 1 -ErrorAction Stop
            """);

        Assert.Empty(ps.Invoke());
        Assert.False(ps.HadErrors, string.Join("; ", ps.Streams.Error));
        Assert.Equal(new string?[] { "first", null }, ReadNames());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void InputBatches_WhatIfAndEmptyInput_DoNotWrite(bool empty)
    {
        using var ps = CreateShell();
        ps.AddScript(empty
            ? "@() | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 2 -PassThru -ErrorAction Stop"
            : "[pscustomobject]@{ Id = 1; Name = 'first' } | Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 1 -WhatIf -ErrorAction Stop");

        var result = ps.Invoke();
        Assert.False(ps.HadErrors, string.Join("; ", ps.Streams.Error));
        if (empty) Assert.Equal(0L, Assert.Single(result).Properties["Rows"].Value);
        else Assert.Empty(result);
        Assert.Empty(ReadNames());
    }

    [Fact]
    public void InputBatches_RejectUnexpandedCollections_BeforeWriting()
    {
        using var ps = CreateShell();
        ps.AddScript("try { Write-DbaXTableData -Provider SQLite -ConnectionString $connection -DestinationTable Records -InputBatchSize 1 -InputObject @(1, 2) -ErrorAction Stop } catch { $_.Exception.Message }");

        Assert.Contains("individual rows", (string)Assert.Single(ps.Invoke()).BaseObject, StringComparison.Ordinal);
        Assert.Empty(ReadNames());
    }

    private PowerShell CreateShell()
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Write-DbaXTableData", typeof(CmdletWriteDbaXTableData), null));
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLite", typeof(CmdletInvokeDbaXSQLite), null));
        state.Variables.Add(new SessionStateVariableEntry("db", _path, null));
        state.Variables.Add(new SessionStateVariableEntry("connection", DBAClientX.SQLite.BuildConnectionString(_path), null));
        return PowerShell.Create(state);
    }

    private string?[] ReadNames()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = DBAClientX.ReturnType.DataTable };
        var table = (DataTable)sqlite.Query(_path, "SELECT Name FROM Records ORDER BY Id")!;
        return table.Rows.Cast<DataRow>().Select(row => row.IsNull(0) ? null : (string)row[0]).ToArray();
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        File.Delete(_path);
        File.Delete(_path + "-wal");
        File.Delete(_path + "-shm");
    }
}
