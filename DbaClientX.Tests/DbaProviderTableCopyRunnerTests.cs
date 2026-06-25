using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class DbaProviderTableCopyRunnerTests
{
    [Fact]
    public async Task CopyAsync_RunsProviderBackedCopyFromRequest()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
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

    [Fact]
    public async Task CopyAsync_BlocksSameProviderTableByDefault()
    {
        var databasePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(databasePath, "INSERT INTO ProbeIndex (ProbeName, LastCompletedUtcMs) VALUES ('Server1', 10);");
            }

            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + databasePath
                },
                Destination = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = databasePath
                },
                Definitions = new[]
                {
                    new DbaTableCopyDefinition("ProbeIndex", "ProbeIndex", new[] { "ProbeName" })
                }
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
            Assert.Contains("Refusing to copy provider table", exception.Message);
        }
        finally
        {
            DeleteIfExists(databasePath);
        }
    }

    [Fact]
    public async Task CopyAsync_BlocksSameSqlServerTableAliasesBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=Monitoring;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=Monitoring;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("dbo.ProbeIndex", "dbo.ProbeIndex", new[] { "ProbeName" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSamePostgreSqlTargetWithDifferentCredentialsBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Port=5432;Database=Monitoring;Username=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Port=5432;Database=Monitoring;Username=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("public.probeindex", "public.probeindex", new[] { "probename" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_AllowsSameProviderDatabaseWithDifferentTables()
    {
        var databasePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE DestinationRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(databasePath, "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One'), (2, 'Two');");
            }

            var result = await new DbaProviderTableCopyRunner().CopyAsync(new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + databasePath
                },
                Destination = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + databasePath + ";Pooling=False"
                },
                Definitions = new[]
                {
                    new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" })
                }
            });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2, result.DestinationRows);
        }
        finally
        {
            DeleteIfExists(databasePath);
        }
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationWhenDestinationIsAlsoASourceTable()
    {
        var databasePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE StagingRows (Id INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE FinalRows (Id INTEGER NOT NULL);");
            }

            var request = new DbaProviderTableCopyRequest
            {
                Source = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = "Data Source=" + databasePath
                },
                Destination = new DbaProviderTableCopyAdapterOptions
                {
                    Provider = DbaTableCopyProvider.SQLite,
                    ConnectionString = databasePath
                },
                Definitions = new[]
                {
                    new DbaTableCopyDefinition("SourceRows", "StagingRows", new[] { "Id" }),
                    new DbaTableCopyDefinition("StagingRows", "FinalRows", new[] { "Id" })
                },
                Options = new DbaTableCopyOptions
                {
                    ClearDestination = true
                }
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
            Assert.Contains("also used as a source table", exception.Message);
        }
        finally
        {
            DeleteIfExists(databasePath);
        }
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private static string CreateTempDatabasePath()
        => Path.Join(Path.GetTempPath(), Path.ChangeExtension(Path.GetRandomFileName(), ".db"));
}
