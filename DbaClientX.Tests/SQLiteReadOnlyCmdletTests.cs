using System.Collections.ObjectModel;
using System.Data;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using DBAClientX;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

/// <summary>
/// Covers R-27: <c>Invoke-DbaXSQLite -ReadOnly</c> opens the database with <c>Mode=ReadOnly</c>.
/// </summary>
public sealed class SQLiteReadOnlyCmdletTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "dbaclientx-readonly-" + Guid.NewGuid().ToString("N") + ".db");

    public SQLiteReadOnlyCmdletTests()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(_path, "CREATE TABLE T (Id INTEGER PRIMARY KEY, Name TEXT); INSERT INTO T VALUES (1, 'a'), (2, 'b');");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadOnly_Select_ReturnsRows(bool stream)
    {
        var results = Invoke(_path, "SELECT Id, Name FROM T ORDER BY Id", ReturnType.DataTable, stream);

        var table = Assert.IsType<DataTable>(Assert.Single(results).BaseObject);
        Assert.Equal(new object[] { "a", "b" }, table.Rows.Cast<DataRow>().Select(row => row["Name"]).ToArray());
    }

    [Fact]
    public void ReadOnly_DefaultReturnType_EmitsPSObjects()
    {
        var results = Invoke(_path, "SELECT Id FROM T ORDER BY Id", returnType: null, stream: false);

        Assert.Equal(new object[] { 1L, 2L }, results.Select(result => result.Properties["Id"].Value).ToArray());
    }

    [Theory]
    [InlineData("INSERT INTO T VALUES (3, 'c')", false)]
    [InlineData("INSERT INTO T VALUES (3, 'c')", true)]
    [InlineData("UPDATE T SET Name = 'z'", false)]
    [InlineData("CREATE TABLE U (Id INTEGER)", false)]
    public void ReadOnly_Write_FailsAndLeavesDatabaseUnchanged(string statement, bool stream)
    {
        var exception = Assert.ThrowsAny<RuntimeException>(() => Invoke(_path, statement, ReturnType.DataTable, stream));

        Assert.True(HasSqliteError(exception, SqliteReadOnly), Flatten(exception));
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
        var table = Assert.IsType<DataTable>(sqlite.Query(_path, "SELECT Name FROM T ORDER BY Id"));
        Assert.Equal(new object[] { "a", "b" }, table.Rows.Cast<DataRow>().Select(row => row["Name"]).ToArray());
        Assert.Equal(0L, sqlite.ExecuteScalar(_path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'U'"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadOnly_MissingDatabase_FailsWithoutCreatingFile(bool stream)
    {
        var missing = Path.Combine(Path.GetTempPath(), "dbaclientx-readonly-missing-" + Guid.NewGuid().ToString("N") + ".db");

        var exception = Assert.ThrowsAny<RuntimeException>(() => Invoke(missing, "SELECT 1", ReturnType.DataTable, stream));

        Assert.True(HasSqliteError(exception, SqliteCannotOpen), Flatten(exception));
        Assert.False(File.Exists(missing));
    }

    [Fact]
    public void ReadOnly_InMemoryDatabase_IsRejected()
    {
        var exception = Assert.ThrowsAny<RuntimeException>(() => Invoke(":memory:", "SELECT 1", ReturnType.DataTable, stream: false));

        Assert.Contains("file-backed", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutReadOnly_Write_Succeeds()
    {
        Invoke(_path, "INSERT INTO T VALUES (3, 'c')", ReturnType.DataTable, stream: false, readOnly: false);

        using var sqlite = new DBAClientX.SQLite();
        Assert.Equal(3L, sqlite.ExecuteScalar(_path, "SELECT COUNT(*) FROM T"));
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var candidate in new[] { _path, _path + "-wal", _path + "-shm" })
        {
            try
            {
                File.Delete(candidate);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    private static Collection<PSObject> Invoke(string database, string query, ReturnType? returnType, bool stream, bool readOnly = true)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLite", typeof(CmdletInvokeDbaXSQLite), helpFileName: null));
        using var powerShell = PowerShell.Create(state);
        powerShell
            .AddCommand("Invoke-DbaXSQLite")
            .AddParameter("Database", database)
            .AddParameter("Query", query)
            .AddParameter("ErrorAction", ActionPreference.Stop);
        if (returnType.HasValue)
        {
            powerShell.AddParameter("ReturnType", returnType.Value);
        }

        if (stream)
        {
            powerShell.AddParameter("Stream");
        }

        if (readOnly)
        {
            powerShell.AddParameter("ReadOnly");
        }

        return powerShell.Invoke();
    }

    /// <summary>SQLITE_READONLY: a write on a read-only connection.</summary>
    private const int SqliteReadOnly = 8;

    /// <summary>SQLITE_CANTOPEN: the database file could not be opened.</summary>
    private const int SqliteCannotOpen = 14;

    /// <summary>
    /// Buffered queries surface <see cref="DbaQueryExecutionException"/>; streamed queries surface the raw <c>SqliteException</c>.
    /// </summary>
    private static bool HasSqliteError(Exception exception, int errorCode)
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            if ((current is DbaQueryExecutionException execution && execution.ProviderErrorCode == errorCode)
                || (current is Microsoft.Data.Sqlite.SqliteException sqlite && sqlite.SqliteErrorCode == errorCode))
            {
                return true;
            }
        }

        return false;
    }

    private static string Flatten(Exception exception)
    {
        var messages = new List<string>();
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            messages.Add(current.Message);
        }

        return string.Join(" | ", messages);
    }
}
