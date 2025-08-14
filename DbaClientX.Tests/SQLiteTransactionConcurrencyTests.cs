using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteTransactionConcurrencyTests
{
    [Fact]
    public async Task CommitAndRollback_AreThreadSafe()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");

        bool commitThrows = false;
        bool rollbackThrows = false;

        var commitTask = Task.Run(() =>
        {
            try
            {
                sqlite.Commit();
            }
            catch (DBAClientX.DbaTransactionException)
            {
                commitThrows = true;
            }
        });

        var rollbackTask = Task.Run(() =>
        {
            try
            {
                sqlite.Rollback();
            }
            catch (DBAClientX.DbaTransactionException)
            {
                rollbackThrows = true;
            }
        });

        await Task.WhenAll(commitTask, rollbackTask);

        Assert.True(commitThrows ^ rollbackThrows);
        Assert.False(sqlite.IsInTransaction);
    }
}
