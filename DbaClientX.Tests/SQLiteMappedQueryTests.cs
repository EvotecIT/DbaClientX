using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;
using Xunit;

namespace DBAClientX.Tests;

public sealed class SQLiteMappedQueryTests
{
    private readonly record struct SampleRow(long Id, string? Name);

    [Fact]
    public async Task QueryReadOnlyAsListAsync_SimpleSelect_ReturnsMappedRows()
    {
        string database = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            var sqlite = new SQLite();

            await sqlite.ExecuteNonQueryAsync(database, "CREATE TABLE t (id INTEGER NOT NULL, name TEXT NULL);");
            await sqlite.ExecuteNonQueryAsync(database, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, NULL);");

            var rows = await sqlite.QueryReadOnlyAsListAsync(
                    database,
                    "SELECT id, name FROM t ORDER BY id;",
                    r => new SampleRow(
                        r.GetInt64(0),
                        r.IsDBNull(1) ? null : r.GetString(1)));

            Assert.Equal(2, rows.Count);
            Assert.Equal(new SampleRow(1, "a"), rows[0]);
            Assert.Equal(new SampleRow(2, null), rows[1]);
        }
        finally
        {
            try
            {
                if (File.Exists(database))
                {
                    File.Delete(database);
                }
            }
            catch
            {
                // Ignore cleanup failures on locked temp files.
            }
        }
    }

    [Fact]
    public async Task QueryReadOnlyAsListAsync_Canceled_Throws()
    {
        string database = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            var sqlite = new SQLite();
            await sqlite.ExecuteNonQueryAsync(database, "CREATE TABLE t (id INTEGER NOT NULL);");
            await sqlite.ExecuteNonQueryAsync(database, "INSERT INTO t (id) VALUES (1);");

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                sqlite.QueryReadOnlyAsListAsync(
                        database,
                        "SELECT id FROM t;",
                        r => r.GetInt64(0),
                        cancellationToken: cts.Token));
        }
        finally
        {
            try
            {
                if (File.Exists(database))
                {
                    File.Delete(database);
                }
            }
            catch
            {
                // Ignore cleanup failures on locked temp files.
            }
        }
    }
}
