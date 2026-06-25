using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class DbaProviderTableCopyRunnerTests
{
    [Fact]
    public async Task CopyAsync_RunsProviderBackedCopyFromRequest()
    {
        var sourcePath = Path.Combine(Path.GetTempPath(), "DbaClientX-source-" + Guid.NewGuid().ToString("N") + ".db");
        var destinationPath = Path.Combine(Path.GetTempPath(), "DbaClientX-destination-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeIndex (ProbeName, LastCompletedUtcMs, StatusId) VALUES ('Server1', 10, 1), ('server1', 20, 2), ('Server2', 15, 3);");
            }

            var result = await new DbaProviderTableCopyRunner().CopyAsync(new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + sourcePath,
                    DefaultOrderByColumns = new[] { "ProbeName" }
                },
                Destination = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + destinationPath
                },
                Definitions = new[]
                {
                    new DbaTableCopyDefinition(
                        "ProbeIndex",
                        "ProbeIndex",
                        new[] { "ProbeName" },
                        SourceOptions: new DbaTableCopySourceOptions(
                            new[] { "ProbeName" },
                            new[] { "LastCompletedUtcMs" },
                            DeduplicateCaseInsensitive: true))
                },
                Options = new DbaTableCopyOptions
                {
                    PageSize = 1,
                    VerifyRowCounts = true
                }
            });

            Assert.True(result.Verified);
            Assert.Equal(2, result.SourceRows);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2, result.DestinationRows);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_RequiresDefinitions()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SQLite,
                ConnectionString = "Data Source=:memory:"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SQLite,
                ConnectionString = "Data Source=:memory:"
            }
        };

        await Assert.ThrowsAsync<ArgumentException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
