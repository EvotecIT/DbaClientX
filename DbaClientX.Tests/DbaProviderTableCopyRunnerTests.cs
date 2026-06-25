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
    public async Task CopyAsync_BlocksSameSqlServerTableWithCurrentDatabaseQualifierBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=tempdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=tempdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "tempdb.dbo.Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameSqlServerTargetWhenDefaultPortIsExplicitBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=db;Database=Monitoring;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=tcp:db,1433;Database=Monitoring;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
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
    public async Task CopyAsync_BlocksSamePostgreSqlTargetWhenDefaultPortIsOmittedBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=reader;Password=one"
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
    public async Task CopyAsync_BlocksSameMySqlTargetWhenDefaultPortIsOmittedBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=Monitoring;User ID=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Port=3306;Database=Monitoring;User ID=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("probeindex", "probeindex", new[] { "probename" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public void NormalizeTableName_PostgreSqlRespectsQuotedIdentifierSemantics()
    {
        var ordinary = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "users");
        var quotedLower = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "\"users\"");
        var quotedMixed = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "\"Users\"");
        var quotedDotted = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "\"tenant.v1\".users");

        Assert.Equal(ordinary, quotedLower);
        Assert.NotEqual(ordinary, quotedMixed);
        Assert.NotEqual(InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "tenant.v1.users"), quotedDotted);
    }

    [Fact]
    public void NormalizeTableName_OracleRespectsQuotedIdentifierSemantics()
    {
        var ordinary = InvokeNormalizeTableName(DbaTableCopyProvider.Oracle, "users");
        var quotedUpper = InvokeNormalizeTableName(DbaTableCopyProvider.Oracle, "\"USERS\"");
        var quotedMixed = InvokeNormalizeTableName(DbaTableCopyProvider.Oracle, "\"Users\"");
        var quotedDotted = InvokeNormalizeTableName(DbaTableCopyProvider.Oracle, "\"APP.V1\".USERS");

        Assert.Equal(ordinary, quotedUpper);
        Assert.NotEqual(ordinary, quotedMixed);
        Assert.NotEqual(InvokeNormalizeTableName(DbaTableCopyProvider.Oracle, "APP.V1.USERS"), quotedDotted);
    }

    [Fact]
    public void NormalizeTableName_SqlServerDropsOnlyCurrentDatabaseQualifier()
    {
        var onePart = InvokeNormalizeTableName(DbaTableCopyProvider.SqlServer, "Rows", "tempdb");
        var currentDatabaseQualified = InvokeNormalizeTableName(DbaTableCopyProvider.SqlServer, "tempdb.dbo.Rows", "tempdb");
        var otherDatabaseQualified = InvokeNormalizeTableName(DbaTableCopyProvider.SqlServer, "OtherDatabase.dbo.Rows", "tempdb");

        Assert.Equal(onePart, currentDatabaseQualified);
        Assert.NotEqual(onePart, otherDatabaseQualified);
    }

    [Fact]
    public void NormalizeTableName_SQLiteDropsMainQualifier()
    {
        var unqualified = InvokeNormalizeTableName(DbaTableCopyProvider.SQLite, "Rows");
        var mainQualified = InvokeNormalizeTableName(DbaTableCopyProvider.SQLite, "main.Rows");

        Assert.Equal(unqualified, mainQualified);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameSQLiteMainQualifiedTableBeforeConnecting()
    {
        var databasePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(databasePath, "CREATE TABLE Rows (Id INTEGER NOT NULL);");
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
                    new DbaTableCopyDefinition("Rows", "main.Rows", new[] { "Id" })
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
    public async Task CopyAsync_BlocksSameOracleUserSchemaTableBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.Oracle,
                ConnectionString = "Data Source=oracle;User Id=app;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.Oracle,
                ConnectionString = "Data Source=oracle;User Id=app;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Users", "APP.Users", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public void TryCreate_SQLiteIdentityPreservesCaseOnCaseSensitiveFileSystems()
    {
        var first = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=" + Path.Join(Path.GetTempPath(), "DbaxCaseSensitive.db")
        };
        var second = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=" + Path.Join(Path.GetTempPath(), "dbaxcasesensitive.db")
        };

        var firstIdentity = InvokeTryCreateIdentity(first);
        var secondIdentity = InvokeTryCreateIdentity(second);

        if (Path.DirectorySeparatorChar == '\\')
        {
            Assert.Equal(firstIdentity, secondIdentity);
        }
        else
        {
            Assert.NotEqual(firstIdentity, secondIdentity);
        }
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

    private static string InvokeNormalizeTableName(DbaTableCopyProvider provider, string tableName)
        => InvokeNormalizeTableName(provider, tableName, null);

    private static string InvokeNormalizeTableName(DbaTableCopyProvider provider, string tableName, string? currentDatabase)
    {
        var type = typeof(DbaProviderTableCopyRunner).Assembly.GetType("DBAClientX.DataMovement.DbaProviderTableCopyTargetIdentity")
            ?? throw new InvalidOperationException("DbaProviderTableCopyTargetIdentity type was not found.");
        var method = type.GetMethod(
                "NormalizeTableName",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic,
                null,
                new[] { typeof(DbaTableCopyProvider), typeof(string), typeof(string) },
                null)
            ?? throw new MissingMethodException(type.FullName, "NormalizeTableName");

        return (string)method.Invoke(null, new object?[] { provider, tableName, currentDatabase })!;
    }

    private static string InvokeTryCreateIdentity(DbaProviderTableCopyAdapterOptions options)
    {
        var type = typeof(DbaProviderTableCopyRunner).Assembly.GetType("DBAClientX.DataMovement.DbaProviderTableCopyTargetIdentity")
            ?? throw new InvalidOperationException("DbaProviderTableCopyTargetIdentity type was not found.");
        var method = type.GetMethod("TryCreate", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
            ?? throw new MissingMethodException(type.FullName, "TryCreate");
        var arguments = new object?[] { options, null };

        var created = (bool)method.Invoke(null, arguments)!;

        Assert.True(created);
        return Assert.IsType<string>(arguments[1]);
    }
}
