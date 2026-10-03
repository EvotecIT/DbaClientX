using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection("SQLite data directory")]
public class SQLiteUriSemanticsTests
{
    [Theory]
    [InlineData("MODE=memory", false)]
    [InlineData("mode=MEMORY", true)]
    [InlineData("cache=SHARED", true)]
    [InlineData("VFS=unregistered-dbax-vfs", false)]
    public void UriOptions_RetainNativeCaseSensitivity(string query, bool invalid)
    {
        string root = CreateRoot();
        try
        {
            string nativePath = Path.Combine(root, "native.db");
            string productPath = Path.Combine(root, "product.db");
            using var native = new SqliteConnection(new SqliteConnectionStringBuilder
                { DataSource = "file:" + Uri.EscapeDataString(nativePath) + "?" + query, Pooling = false }.ToString());
            using var sqlite = new SQLite();
            if (invalid)
            {
                Assert.Throws<SqliteException>(() => native.Open());
                Assert.Throws<DbaQueryExecutionException>(() => sqlite.ExecuteScalar(
                    "file:" + Uri.EscapeDataString(productPath) + "?" + query, "SELECT 1"));
                Assert.False(File.Exists(productPath));
            }
            else
            {
                native.Open();
                sqlite.ExecuteNonQuery("file:" + Uri.EscapeDataString(productPath) + "?" + query,
                    "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
                Assert.True(File.Exists(nativePath));
                Assert.True(File.Exists(productPath));
                Assert.Equal(3L, sqlite.ExecuteScalar(productPath, "SELECT COUNT(*) FROM items"));
            }
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("mode=memory&mode=rwc", true)]
    [InlineData("mode=rwc&mode=memory", false)]
    [InlineData("%6dode=memory&mode=rwc", true)]
    public void RepeatedUriModes_UseTheLastOccurrence(string query, bool disk)
    {
        string root = CreateRoot();
        try
        {
            string nativePath = Path.Combine(root, "native.db");
            string productPath = Path.Combine(root, "product.db");
            using (var native = new SqliteConnection(new SqliteConnectionStringBuilder
                { DataSource = "file:" + Uri.EscapeDataString(nativePath) + "?" + query, Pooling = false }.ToString()))
            {
                native.Open();
                using var command = native.CreateCommand();
                command.CommandText = "CREATE TABLE items(id INTEGER)";
                command.ExecuteNonQuery();
            }
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery("file:" + Uri.EscapeDataString(productPath) + "?" + query,
                "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            Assert.Equal(disk, File.Exists(nativePath));
            Assert.Equal(disk, File.Exists(productPath));
            if (disk) Assert.Equal(3L, sqlite.ExecuteScalar(productPath, "SELECT COUNT(*) FROM items"));
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("cache=private&cache=shared", true)]
    [InlineData("cache=shared&cache=private", false)]
    public void RepeatedUriCacheOptions_MatchNativeNamespaces(string query, bool shared)
    {
        string name = "dbax-cache-" + Guid.NewGuid().ToString("N");
        using var native = new SqliteConnection(new SqliteConnectionStringBuilder
            { DataSource = "file:" + name + "?mode=memory&cache=shared", Pooling = false }.ToString());
        native.Open();
        using var command = native.CreateCommand();
        command.CommandText = "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)";
        command.ExecuteNonQuery();
        using var sqlite = new SQLite();
        string uri = "file:" + name + "?mode=memory&" + query;
        Assert.Equal(shared ? 1L : 0L, sqlite.ExecuteScalar(uri,
            "SELECT COUNT(*) FROM sqlite_master WHERE name='items'"));
    }

    [Theory]
    [InlineData("FILE:items.db")]
    [InlineData("File:items.db")]
    [InlineData(":MEMORY:")]
    public async Task UppercaseNativeFilename_RemainsAnOrdinaryFilenameAndCopyTarget(string filename)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Colon filenames require POSIX.");
        string root = CreateRoot();
        string previous = Environment.CurrentDirectory;
        try
        {
            Environment.CurrentDirectory = root;
            using (var native = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = filename, Pooling = false }.ToString()))
            {
                native.Open();
                using var command = native.CreateCommand();
                command.CommandText = "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)";
                command.ExecuteNonQuery();
            }
            using var sqlite = new SQLite();
            Assert.Equal(3L, sqlite.ExecuteScalar(filename, "SELECT COUNT(*) FROM items"));
            Assert.False(File.Exists("items.db"));
            var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"),
                _ => throw new Exception("Must reject before connecting"));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = filename },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = Path.Combine(root, filename) },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true }, AllowSameProviderTableCopy = true
            };
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", error.Message);
            Assert.Equal(3L, sqlite.ExecuteScalar(filename, "SELECT COUNT(*) FROM items"));
        }
        finally { Environment.CurrentDirectory = previous; Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Maintenance_LinkBeforeParentSegmentChecksTheNativeTarget(bool uri, bool missing)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Exercises POSIX link-before-parent resolution.");
        string root = CreateRoot();
        string physical = Path.Combine(root, "physical");
        string nested = Path.Combine(physical, "nested");
        Directory.CreateDirectory(nested);
        string alias = Path.Combine(root, "alias");
        Directory.CreateSymbolicLink(alias, nested);
        string actual = Path.Combine(physical, "items.db");
        string lexical = Path.Combine(root, "items.db");
        string aliased = Path.Combine(alias, "..", "items.db");
        string target = uri ? "file:" + aliased : aliased;
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(missing ? lexical : actual, "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            if (missing)
            {
                await Assert.ThrowsAsync<FileNotFoundException>(() => sqlite.CheckpointAsync(target));
                await Assert.ThrowsAsync<FileNotFoundException>(() => sqlite.OptimizeAsync(target));
                await Assert.ThrowsAsync<FileNotFoundException>(() => sqlite.PrepareForShutdownAsync(target));
                Assert.False(File.Exists(actual));
                Assert.Equal(3L, sqlite.ExecuteScalar(lexical, "SELECT COUNT(*) FROM items"));
            }
            else
            {
                await sqlite.CheckpointAsync(target);
                await sqlite.OptimizeAsync(target);
                await sqlite.PrepareForShutdownAsync(target);
                Assert.Equal(3L, sqlite.ExecuteScalar(actual, "SELECT COUNT(*) FROM items"));
                Assert.False(File.Exists(lexical));
            }
        }
        finally { Directory.Delete(alias); Directory.Delete(root, true); }
    }

    [Fact]
    public void RepeatedMode_CannotEscalateNativeReadOnlyAccess()
    {
        string root = CreateRoot();
        string path = Path.Combine(root, "items.db");
        string uri = "file:" + Uri.EscapeDataString(path) + "?mode=ro&mode=rwc";
        try
        {
            using var native = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = uri, Pooling = false }.ToString());
            Assert.Throws<SqliteException>(() => native.Open());
            using var sqlite = new SQLite();
            Assert.Throws<ArgumentException>(() => sqlite.ExecuteNonQuery(uri, "CREATE TABLE items(id INTEGER)"));
            Assert.False(File.Exists(path));
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedOptions_RejectClearingTheEffectiveNativeSource(bool memory)
    {
        string root = CreateRoot();
        string path = Path.Combine(root, "items.db");
        string uri = "file:" + Uri.EscapeDataString(path) + (memory
            ? "?mode=memory&cache=private&cache=shared" : "?mode=memory&mode=rwc");
        string destination = memory ? "FullUri=file:" + Uri.EscapeDataString(path) + "?mode=memory&cache=shared;Pooling=False" : path;
        try
        {
            using var sqlite = new SQLite();
            using var held = sqlite.OpenSession(uri);
            held.ExecuteNonQuery("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            Assert.Equal(3L, memory ? sqlite.ExecuteScalarWithConnectionString(destination, "SELECT COUNT(*) FROM items")
                : sqlite.ExecuteScalar(destination, "SELECT COUNT(*) FROM items"));
            var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"),
                _ => throw new Exception("Must reject before connecting"));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = uri },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = destination },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true }, AllowSameProviderTableCopy = true
            };
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", error.Message);
            Assert.Equal(3L, held.ExecuteScalar("SELECT COUNT(*) FROM items"));
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("unregistered-dbax-vfs", false)]
    [InlineData("", false)]
    [InlineData("", true)]
    public void RepeatedVfs_UsesTheLastNativeSelectionIncludingEmpty(string initial, bool emptyLast)
    {
        string root = CreateRoot();
        string vfs = OperatingSystem.IsWindows() ? "win32" : "unix";
        string query = emptyLast ? "vfs=" + vfs + "&vfs=" : "vfs=" + initial + "&vfs=" + vfs;
        try
        {
            string nativeUri = "file:" + Uri.EscapeDataString(Path.Combine(root, "native.db")) + "?" + query;
            string productUri = "file:" + Uri.EscapeDataString(Path.Combine(root, "product.db")) + "?" + query;
            using var native = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = nativeUri, Pooling = false }.ToString());
            using var sqlite = new SQLite();
            if (emptyLast)
            {
                Assert.Throws<SqliteException>(() => native.Open());
                Assert.Throws<DbaQueryExecutionException>(() => sqlite.ExecuteScalar(productUri, "SELECT 1"));
                Assert.False(File.Exists(Path.Combine(root, "product.db")));
            }
            else
            {
                native.Open();
                Assert.Equal(1L, sqlite.ExecuteScalar(productUri, "SELECT 1"));
            }
        }
        finally { Directory.Delete(root, true); }
    }

    private static string CreateRoot()
    {
        string root = Path.Combine(Path.GetTempPath(), "dbax-semantics-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
    }
}
