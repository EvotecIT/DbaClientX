using DBAClientX;
using System.Threading;

namespace DbaClientX.Tests;

public class SQLiteQueryColumnsTests
{
    [Fact]
    public void PreparedColumnsDoNotEvaluateParametersOrFunctionsAndKeepDuplicateLabels()
    {
        using var client = new SQLite { ConnectionOptions = new SQLiteConnectionOptions { Pooling = false, EnableWriteAheadLogging = false } };
        string database = "file:query-columns-" + Guid.NewGuid().ToString("N") + "?mode=memory&cache=shared";
        using var connection = client.OpenDbConnection(database);
        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE items (name TEXT, amount REAL, enabled BOOLEAN); INSERT INTO items VALUES ('before', 1, 1);";
        create.ExecuteNonQuery();
        var columns = client.GetQueryColumns(database,
            "SELECT name AS \"Żółć\", amount AS duplicate, enabled AS duplicate, abs(-9223372036854775808) FROM items LIMIT @limit");
        Assert.Equal(new[] { "Żółć", "duplicate", "duplicate", "abs(-9223372036854775808)" }, columns.Select(column => column.Name));
        Assert.Equal(new[] { "TEXT", "REAL", "BOOLEAN", "" }, columns.Select(column => column.DeclaredTypeName));
        // Inspecting mutating SQL also must not execute it or a later batch statement.
        Assert.Empty(client.GetQueryColumns(database, "DELETE FROM items; DROP TABLE items;"));
        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT name FROM items";
        Assert.Equal("before", verify.ExecuteScalar());
    }

    [Fact]
    public void InvalidSqlAndCanceledPreparationReleaseTheOwnedConnection()
    {
        using var client = new SQLite();
        Assert.Throws<DbaQueryExecutionException>(() => client.GetQueryColumns(":memory:", "SELECT missing FROM absent"));
        Assert.Single(client.GetQueryColumns(":memory:", "SELECT 1"));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() => client.GetQueryColumns(":memory:", "SELECT 1", cancellation.Token));
    }
}
