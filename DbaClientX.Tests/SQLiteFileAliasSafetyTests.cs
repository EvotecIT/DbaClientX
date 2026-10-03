using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class SQLiteFileAliasSafetyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PosixDirectoryLinkBeforeParentSegment_RejectsClearingItsSource(bool fileUri)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Exercises POSIX link-before-parent resolution.");
        string root = Path.Combine(Path.GetTempPath(), "dbax-parent-" + Guid.NewGuid().ToString("N"));
        string physical = Path.Combine(root, "physical space ü");
        string nested = Path.Combine(physical, "nested");
        Directory.CreateDirectory(nested);
        string alias = Path.Combine(root, "alias");
        Directory.CreateSymbolicLink(alias, nested);
        string database = Path.Combine(physical, "items.db");
        string aliasedDatabase = Path.Combine(alias, "..", "items.db");
        // System.Uri removes dot segments itself; retain the literal native URI path.
        string destination = fileUri ? "FullUri=file:" + aliasedDatabase + ";Pooling=False" : aliasedDatabase;
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(database, "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            Assert.Equal(3L, fileUri
                ? sqlite.ExecuteScalarWithConnectionString(destination, "SELECT COUNT(*) FROM items")
                : sqlite.ExecuteScalar(destination, "SELECT COUNT(*) FROM items"));
            var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = database },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = destination },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }, AllowSameProviderTableCopy = true
            };
            var copyError = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", copyError.Message);
            Assert.Equal(3L, sqlite.ExecuteScalar(database, "SELECT COUNT(*) FROM items"));
        }
        finally { Directory.Delete(alias); Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FilesystemAliases_RejectDestructiveCopyAndBackup(bool fileAlias)
    {
        string root = Path.Combine(Path.GetTempPath(), "dbax-link-" + Guid.NewGuid().ToString("N"));
        string directory = Path.Combine(root, "physical space ü");
        Directory.CreateDirectory(directory);
        string database = Path.Combine(directory, "items.db");
        string alias = Path.Combine(root, "alias");
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(database, "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            try
            {
                if (fileAlias) File.CreateSymbolicLink(alias, database);
                else Directory.CreateSymbolicLink(alias, directory);
            }
            catch (UnauthorizedAccessException) { Assert.Skip("Symbolic-link creation requires unavailable permission."); }
            catch (IOException ex) when (OperatingSystem.IsWindows() && (ex.HResult & 0xffff) == 1314)
            { Assert.Skip("Symbolic-link creation requires unavailable permission."); }
            string aliasedDatabase = fileAlias ? alias : Path.Combine(alias, "items.db");
            var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = database },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = aliasedDatabase },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }, AllowSameProviderTableCopy = true
            };
            var copyError = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", copyError.Message);
            var backupError = Assert.Throws<ArgumentException>(() => sqlite.BackupDatabase(database, aliasedDatabase, overwriteDestination: true));
            Assert.Equal("destinationDatabase", backupError.ParamName);
            Assert.Equal(3L, sqlite.ExecuteScalar(database, "SELECT COUNT(*) FROM items"));
            // Missing destinations below an aliased directory are safe new files, not the source.
            if (!fileAlias)
            {
                string backup = Path.Combine(alias, "backup.db");
                sqlite.BackupDatabase(database, backup);
                Assert.Equal(3L, sqlite.ExecuteScalar(backup, "SELECT COUNT(*) FROM items"));
            }
        }
        finally
        {
            if (fileAlias) File.Delete(alias);
            else if (Directory.Exists(alias)) Directory.Delete(alias);
            Directory.Delete(root, true);
        }
    }
}
