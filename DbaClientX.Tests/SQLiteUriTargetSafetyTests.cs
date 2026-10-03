using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection("SQLite data directory")]
public class SQLiteUriTargetSafetyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SharedMemoryUri_FromLongWorkingDirectory_RemainsInMemory(bool connectionInput)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Exercises the Windows journal path threshold.");
        string root = Path.Combine(Path.GetTempPath(), "dbax-uri-" + Guid.NewGuid().ToString("N"));
        string directory = root;
        while (directory.Length < 250)
            directory = Path.Combine(directory, new string('x', Math.Min(48, 249 - directory.Length)));
        Directory.CreateDirectory(directory);
        string previous = Environment.CurrentDirectory;
        try
        {
            Environment.CurrentDirectory = directory;
            using var sqlite = new SQLite();
            using var first = sqlite.OpenSession("file::memory:?cache=shared");
            first.ExecuteNonQuery("CREATE TABLE uri_items(value INTEGER); INSERT INTO uri_items VALUES(1)");
            object? count = connectionInput
                ? sqlite.ExecuteScalarWithConnectionString("FullUri=file::memory:?cache=shared;Pooling=False", "SELECT COUNT(*) FROM uri_items")
                : sqlite.ExecuteScalar("file::memory:?cache=shared", "SELECT COUNT(*) FROM uri_items");
            Assert.Equal(1L, count);
            Assert.Empty(Directory.EnumerateFiles(directory));
        }
        finally { Environment.CurrentDirectory = previous; Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("file::memory:?cache=shared", false)]
    [InlineData("file::memory:?cache=shared", true)]
    [InlineData("file:file%3Aguard?mode=memory&cache=shared", false)]
    [InlineData("file:file%3Aguard?mode=memory&cache=shared", true)]
    public async Task SharedMemoryUri_RejectsClearingItsSourceBeforeConnecting(string uri, bool normalizedTarget)
    {
        var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = uri },
            Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = normalizedTarget ? SQLite.BuildConnectionString(uri) : "FullUri=" + uri + ";Pooling=False" },
            Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
            Options = new DbaTableCopyOptions { ClearDestination = true },
            AllowSameProviderTableCopy = true
        };
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
        Assert.Contains("also used as a source table", exception.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DecodedDiskFilenameStartingWithFileScheme_RejectsClearingItsSource(bool directoryAlias)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Colon filenames require a POSIX filesystem.");
        string root = Path.Combine(Path.GetTempPath(), "dbax-opaque-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        string directory = Path.Combine(root, "physical space ü");
        Directory.CreateDirectory(directory);
        string alias = Path.Combine(root, "alias");
        if (directoryAlias) Directory.CreateSymbolicLink(alias, directory);
        string previous = Environment.CurrentDirectory;
        try
        {
            Environment.CurrentDirectory = directoryAlias ? alias : directory;
            const string uri = "file:file%3Aitems.db";
            string path = Path.Combine(directoryAlias ? alias : directory, "file:items.db");
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(uri, "CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1),(2),(3)");
            Assert.Equal(3L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM items"));
            var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = uri },
                Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = path },
                Definitions = new[] { new DbaTableCopyDefinition("items", "items", new[] { "id" }) },
                Options = new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 },
                AllowSameProviderTableCopy = true
            };
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => runner.CopyAsync(request));
            Assert.Contains("also used as a source table", exception.Message);
            Assert.Equal(3L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM items"));
        }
        finally
        {
            Environment.CurrentDirectory = previous;
            if (directoryAlias) Directory.Delete(alias);
            Directory.Delete(root, true);
        }
    }

    [Theory]
    [InlineData("remotehost.invalid")]
    [InlineData("LOCALHOST")]
    [InlineData("127.0.0.1")]
    public async Task UnsupportedUriAuthorities_AreRejectedBeforeConnecting(string authority)
    {
        string uri = "file://" + authority + "/share/data.db";
        Assert.Throws<ArgumentException>(() => SQLite.BuildConnectionString(uri));
        using var sqlite = new SQLite();
        Assert.Throws<ArgumentException>(() => sqlite.ExecuteScalarWithConnectionString("FullUri=" + uri, "SELECT 1"));
        var runner = new DbaProviderTableCopyRunner(_ => throw new Exception("Must reject before connecting"), _ => throw new Exception("Must reject before connecting"));
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = uri },
            Destination = new DbaProviderTableCopyAdapterOptions { Provider = DbaTableCopyProvider.SQLite, ConnectionString = "Data Source=local.db" },
            Definitions = new[] { new DbaTableCopyDefinition("items", "copy", new[] { "id" }) }
        };
        await Assert.ThrowsAsync<ArgumentException>(() => runner.CopyAsync(request));
    }

    [Fact]
    public void LocalhostUri_OpensTheLocalFileAndRetainsReadOnlyOptions()
    {
        string path = Path.Combine(Path.GetTempPath(), "dbax-localhost-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items(value INTEGER); INSERT INTO items VALUES(1)");
            string uri = new Uri(path).AbsoluteUri.Replace("file:///", "file://localhost/") + "?mode=ro";
            Assert.Equal(1L, sqlite.ExecuteScalar(uri, "SELECT COUNT(*) FROM items"));
            Assert.Throws<DbaQueryExecutionException>(() => sqlite.ExecuteNonQuery(uri, "INSERT INTO items VALUES(2)"));
            Assert.Equal(1L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM items"));
        }
        finally { File.Delete(path); File.Delete(path + "-wal"); File.Delete(path + "-shm"); }
    }
}
