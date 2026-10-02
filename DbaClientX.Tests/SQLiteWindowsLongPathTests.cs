using DBAClientX;
using DBAClientX.DataMovement;
using System.Data.Common;

namespace DbaClientX.Tests;

public class SQLiteWindowsLongPathTests
{
    [Theory]
    [InlineData(252, "path")]
    [InlineData(1100, "connection")]
    [InlineData(320, "uri")]
    [InlineData(320, "relative")]
    public async Task LongFilePaths_PreserveWritesRollbackAndReadOnlyReopen(int length, string inputKind)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises native Windows path and journal handling.");
        string root = Path.Combine(Path.GetTempPath(), "dbax-long-" + Guid.NewGuid().ToString("N"));
        string directory = Path.Combine(root, "space # % ü");
        while (directory.Length < length - 9)
            directory = Path.Combine(directory, new string('a', Math.Max(1, Math.Min(48, length - 10 - directory.Length))));
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "store.db");
        try
        {
            using var sqlite = new SQLite();
            const string create = "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'retained');";
            if (inputKind is "path" or "relative")
                await sqlite.ExecuteNonQueryAsync(inputKind == "path" ? path : Path.GetRelativePath(Environment.CurrentDirectory, path), create);
            else
            {
                string source = inputKind == "uri" ? "FullUri=" + new Uri(path).AbsoluteUri : "Data Source=" + path;
                await sqlite.ExecuteNonQueryWithConnectionStringAsync(source + ";Pooling=False", create);
            }

            using (DbConnection connection = sqlite.OpenDbConnection(path))
            {
                using DbCommand command = connection.CreateCommand();
                command.CommandText = "PRAGMA journal_mode";
                Assert.Equal("wal", command.ExecuteScalar());
            }
            await using (var session = await sqlite.OpenSessionAsync(path))
                Assert.Equal("retained", await session.ExecuteScalarAsync("SELECT value FROM items WHERE id=1"));
            using (var session = sqlite.OpenSession(path))
            {
                Assert.Throws<InvalidOperationException>(() => session.RunInTransaction(tx =>
                {
                    tx.ExecuteNonQuery("INSERT INTO items VALUES(2, 'rollback')");
                    throw new InvalidOperationException("Abort write");
                }));
            }
            using (DbConnection connection = sqlite.OpenDbConnection(path, new SQLiteConnectionOptions { ReadOnly = true }))
            {
                using DbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM items";
                Assert.Equal(1L, command.ExecuteScalar());
                command.CommandText = "INSERT INTO items VALUES(3, 'rejected')";
                Assert.Throws<Microsoft.Data.Sqlite.SqliteException>(() => command.ExecuteNonQuery());
            }
            Assert.True(File.Exists(path));
            var adapter = new SQLiteTableCopyAdapter("Data Source=" + path + ";Pooling=False", new[] { "id" });
            using var page = await adapter.ReadPageAsync(new DbaTableCopyPageRequest(
                new DbaTableCopyDefinition("items", "copy", new[] { "id" }), null, 10));
            Assert.Single(page.Data.Rows.Cast<System.Data.DataRow>());
            Assert.Equal("retained", page.Data.Rows[0]["value"]);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void BuildConnectionString_LongUncPathPreservesShareIdentity()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises Windows UNC path construction without network access.");
        string path = @"\\server\share\" + string.Join("\\", Enumerable.Repeat(new string('x', 48), 6)) + @"\data.db";
        var builder = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(SQLite.BuildConnectionString(path));
        Assert.Equal(@"\\?\UNC\" + path.Substring(2), builder.DataSource);
        Assert.Equal("win32-longpath", builder.Vfs);
    }
}
