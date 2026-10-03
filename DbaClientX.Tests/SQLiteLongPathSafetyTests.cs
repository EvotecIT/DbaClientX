using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection("SQLite data directory")]
public class SQLiteLongPathSafetyTests
{
    [Theory]
    [InlineData(false, "path")]
    [InlineData(true, "path")]
    [InlineData(false, "uri")]
    [InlineData(false, "data-directory")]
    public async Task MixedExtendedPathAliases_RejectClearBeforeRemovingSourceRows(bool reverse, string inputKind)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises Windows filename aliases.");
        var (root, directory) = CreateLongDirectory();
        string path = Path.Combine(directory, "store.db");
        object? oldDirectory = AppDomain.CurrentDomain.GetData("DataDirectory");
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1),(2),(3);");
            string built = SQLite.BuildConnectionString(path);
            string alias = inputKind == "uri" ? new Uri(path).AbsoluteUri + "?immutable=0" : path;
            if (inputKind == "data-directory")
            {
                AppDomain.CurrentDomain.SetData("DataDirectory", directory);
                alias = "|DataDirectory|store.db";
            }
            var runner = new DbaProviderTableCopyRunner(options => new SQLiteTableCopyAdapter(options), options => new SQLiteTableCopyAdapter(options));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = reverse ? alias : built },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = reverse ? built : alias },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 },
                AllowSameProviderTableCopy = true
            };
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", exception.Message);
            Assert.Equal(3L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM items"));
        }
        finally { AppDomain.CurrentDomain.SetData("DataDirectory", oldDirectory); Directory.Delete(root, true); }
    }

    [Fact]
    public void LongRawMemoryUri_DoesNotCreateAFileAndSharesNamedMemory()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises Windows long raw URI handling.");
        var (root, directory) = CreateLongDirectory();
        string path = Path.Combine(directory, "memory.db");
        try
        {
            string connectionString = SQLite.BuildConnectionString(new Uri(path).AbsoluteUri + "?mode=memory&cache=shared");
            using var first = new SqliteConnection(connectionString);
            using var second = new SqliteConnection(connectionString);
            first.Open(); second.Open();
            using var create = first.CreateCommand();
            create.CommandText = "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1);";
            create.ExecuteNonQuery();
            Assert.False(File.Exists(path));
            using var query = second.CreateCommand();
            query.CommandText = "SELECT COUNT(*) FROM items";
            Assert.Equal(1L, query.ExecuteScalar());
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("mode=ro", false)]
    [InlineData("immutable=1", false)]
    [InlineData("immutable=1", true)]
    public void LongReadOnlyUri_RejectsWrites(string query, bool connectionInput)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises Windows long file URI handling.");
        var (root, directory) = CreateLongDirectory();
        string path = Path.Combine(directory, "readonly.db");
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1);");
            string uri = new Uri(path).AbsoluteUri + "?" + query;
            if (connectionInput)
            {
                string input = "FullUri=" + uri + ";Pooling=False";
                Assert.Equal(1L, sqlite.ExecuteScalarWithConnectionString(input, "SELECT COUNT(*) FROM items"));
                Assert.Throws<DbaQueryExecutionException>(() => sqlite.ExecuteNonQueryWithConnectionString(input, "INSERT INTO items VALUES(2)"));
            }
            else
            {
                using var session = sqlite.OpenSession(uri);
                Assert.Equal(1L, session.ExecuteScalar("SELECT COUNT(*) FROM items"));
                Assert.Throws<DbaQueryExecutionException>(() => session.ExecuteNonQuery("INSERT INTO items VALUES(2)"));
            }
            Assert.Equal(1L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM items"));
        }
        finally { Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData(@"C:\ordinary\store.db", @"\\?\C:\ordinary\store.db")]
    public async Task ExtendedAliases_BlockConsistentSameDatabaseCopyBeforeConnecting(string ordinary, string extended)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises Windows drive aliases before connecting.");
        var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = ordinary, ReadConsistency = DbaTableCopyReadConsistency.Snapshot },
            Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = "Data Source=" + extended },
            Definitions = new[] { new DbaTableCopyDefinition("items", "copy", new[] { "id" }) }
        };
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
        Assert.Contains("CallerManaged", exception.Message);
        Assert.True(SQLite.AreSameBackupPath(ordinary, extended));
        Assert.False(SQLite.AreSameBackupPath(ordinary, extended + ".other"));
    }

    [Fact]
    public void RawUri_PreservesVfsAndExplicitConnectionOptions()
    {
        string uri = new Uri(Path.Combine(Path.GetTempPath(), "settings.db")).AbsoluteUri + "?mode=ro&cache=shared&vfs=caller-vfs&immutable=1";
        var raw = new SqliteConnectionStringBuilder(SQLite.BuildConnectionString(uri));
        Assert.Equal(SqliteOpenMode.ReadOnly, raw.Mode);
        Assert.Equal(SqliteCacheMode.Shared, raw.Cache);
        Assert.Equal("caller-vfs", raw.Vfs);
        var method = typeof(SQLite).GetMethod("NormalizeConnectionString", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
        var explicitOptions = new SqliteConnectionStringBuilder((string)method.Invoke(null, new object[] { "DataSource=" + uri + ";Mode=ReadWrite;Cache=Private;Vfs=explicit-vfs", false })!);
        Assert.Equal(SqliteOpenMode.ReadWrite, explicitOptions.Mode);
        Assert.Equal(SqliteCacheMode.Private, explicitOptions.Cache);
        Assert.Equal("explicit-vfs", explicitOptions.Vfs);
        Assert.Contains("immutable=1", explicitOptions.DataSource);
    }

    [Theory]
    [InlineData("#one", "#two", "raw-uri")]
    [InlineData("%41", "A", "raw-uri")]
    [InlineData("#one", "#two", "full-uri")]
    [InlineData("#one", "#two", "memory-name")]
    [InlineData("?one", "?two", "memory-name")]
    public void EscapedMemoryUriNames_RemainIsolated(string firstSuffix, string secondSuffix, string inputKind)
    {
        string prefix = Path.Combine(Path.GetTempPath(), "dbax-memory-" + Guid.NewGuid().ToString("N"));
        string firstUri = new Uri(prefix).AbsoluteUri + Uri.EscapeDataString(firstSuffix) + "?mode=memory&cache=shared";
        string secondUri = new Uri(prefix).AbsoluteUri + Uri.EscapeDataString(secondSuffix) + "?mode=memory&cache=shared";
        string ConnectionString(string uri, string suffix)
        {
            if (inputKind == "raw-uri") return SQLite.BuildConnectionString(uri);
            var normalize = typeof(SQLite).GetMethod("NormalizeConnectionString", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
            string input = inputKind == "full-uri" ? "FullUri=" + uri + ";Pooling=False"
                : new SqliteConnectionStringBuilder { DataSource = prefix + suffix, Mode = SqliteOpenMode.Memory, Cache = SqliteCacheMode.Shared, Pooling = false }.ToString();
            return (string)normalize.Invoke(null, new object[] { input, false })!;
        }
        using var first = new SqliteConnection(ConnectionString(firstUri, firstSuffix));
        using var second = new SqliteConnection(ConnectionString(secondUri, secondSuffix));
        first.Open(); second.Open();
        using var create = first.CreateCommand();
        create.CommandText = "CREATE TABLE private_items(value TEXT); INSERT INTO private_items VALUES('first');";
        create.ExecuteNonQuery();
        using var query = second.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name='private_items'";
        Assert.Equal(0L, query.ExecuteScalar());
    }

    private static (string Root, string Directory) CreateLongDirectory()
    {
        string root = Path.Combine(Path.GetTempPath(), "dbax-safety-" + Guid.NewGuid().ToString("N"));
        string directory = Path.Combine(root, "space # % ü");
        while (directory.Length < 330) directory = Path.Combine(directory, new string('a', 48));
        Directory.CreateDirectory(directory);
        return (root, directory);
    }
}

// DataDirectory is process-wide provider configuration, so these cases must not overlap other tests.
[CollectionDefinition("SQLite data directory", DisableParallelization = true)]
public class SQLiteDataDirectoryCollection { }
