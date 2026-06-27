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

            var result = await CreateRunner().CopyAsync(new DbaProviderTableCopyRequest
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

        await Assert.ThrowsAsync<ArgumentException>(() => CreateRunner().CopyAsync(request));
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

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("unqualified", exception.Message);
        Assert.Contains("default schema is unknown", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameSqlServerTableWithCasePreservedCurrentDatabaseQualifierBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("dbo.Rows", "App.dbo.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to clear destination table", exception.Message);
    }

    [Fact]
    public void ValidateSameProviderTableCopy_BlocksSqlServerCaseOnlyTableDifference()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=App;Integrated Security=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("dbo.Rows", "App.dbo.rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = Assert.Throws<InvalidOperationException>(() => InvokeValidateSameProviderTableCopy(request));
        Assert.Contains("Refusing to clear destination table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameSqlServerDatabaseWithCaseOnlyConnectionDifferenceBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=app;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("dbo.Rows", "dbo.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksExplicitSqlServerCrossDatabaseSameTableClearBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=Other;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Shared.dbo.Rows", "Shared.dbo.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksExplicitSqlServerCrossDatabaseSameTableCopyBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=Other;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Shared.dbo.Rows", "Shared.dbo.Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public void ValidateSameProviderTableCopy_AllowsExplicitSqlServerDifferentDatabaseSourceToUnqualifiedDestination()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Archive.dbo.Rows", "Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        InvokeValidateSameProviderTableCopy(request);
    }

    [Fact]
    public void ValidateSameProviderTableCopy_AllowsExplicitSqlServerSameDatabaseTableOnDifferentServers()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=prod;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=staging;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("App.dbo.Rows", "App.dbo.Rows", new[] { "Id" })
            }
        };

        InvokeValidateSameProviderTableCopy(request);
    }

    [Fact]
    public void ValidateSameProviderTableCopy_AllowsUnqualifiedSqlServerSameTableOnDifferentServers()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=prod;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=staging;Database=App;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "Rows", new[] { "Id" })
            }
        };

        InvokeValidateSameProviderTableCopy(request);
    }

    [Fact]
    public async Task CopyAsync_BlocksExplicitMySqlCrossDatabaseSameTableClearBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=App;User ID=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=Other;User ID=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Shared.Rows", "Shared.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksExplicitMySqlCrossDatabaseSameTableCopyBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=App;User ID=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=Other;User ID=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Shared.Rows", "Shared.Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_RejectsMySqlDestinationWithoutLocalInfileBeforeClearing()
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
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=app;User ID=writer;Password=two;SslMode=Required"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            },
            AllowSameProviderTableCopy = true
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("AllowLoadLocalInfile=true", exception.Message);
        Assert.Contains("Allow Load Local Infile=true", exception.Message);
    }

    [Theory]
    [InlineData("AllowLoadLocalInfile=true")]
    [InlineData("Allow Load Local Infile=1")]
    public void ValidateDestinationBulkCopyRequirements_AllowsMySqlDestinationWithLocalInfile(string option)
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
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = $"Server=localhost;Database=app;User ID=writer;Password=two;SslMode=Required;{option}"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "Rows", new[] { "Id" })
            }
        };

        InvokeValidateDestinationBulkCopyRequirements(request);
    }

    [Fact]
    public void ValidateDestinationBulkCopyRequirements_RejectsUnsupportedMySqlLoadLocalInfileAlias()
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
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=app;User ID=writer;Password=two;SslMode=Required;LoadLocalInfile=true"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "Rows", new[] { "Id" })
            }
        };

        var exception = Assert.Throws<InvalidOperationException>(() => InvokeValidateDestinationBulkCopyRequirements(request));
        Assert.Contains("AllowLoadLocalInfile=true", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameMySqlDatabaseWithCaseOnlyConnectionDifferenceBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=App;User ID=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=app;User ID=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_RejectsUnqualifiedSqlServerClearWhenDefaultSchemaIsUnknown()
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
                new DbaTableCopyDefinition("Rows", "app.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("unqualified", exception.Message);
        Assert.Contains("default schema is unknown", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_RejectsUnqualifiedSqlServerSelfCopyWhenDefaultSchemaIsUnknown()
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
                new DbaTableCopyDefinition("Rows", "app.Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("unqualified", exception.Message);
        Assert.Contains("default schema is unknown", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_RejectsOverlappingUnqualifiedSqlServerClearEvenWhenSameProviderCopyIsAllowed()
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
                new DbaTableCopyDefinition("Rows", "app.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            },
            AllowSameProviderTableCopy = true
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("unqualified", exception.Message);
        Assert.Contains("default schema is unknown", exception.Message);
    }

    [Fact]
    public void ValidateSameProviderTableCopy_AllowsDifferentUnqualifiedSqlServerTablesBeforeConnecting()
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
                new DbaTableCopyDefinition("Rows", "ArchiveRows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            },
            AllowSameProviderTableCopy = true
        };

        InvokeValidateSameProviderTableCopy(request);
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSamePostgreSqlPublicSchemaAliasesBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=reader;Password=one;Search Path=public"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path=public"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("users", "public.users", new[] { "id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSamePostgreSqlSearchPathSchemaBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=reader;Password=one;Search Path=tenant"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path=tenant"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "tenant.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to clear destination table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSamePostgreSqlQuotedSearchPathSchemaBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=reader;Password=one;SslMode=Require;Search Path=\"Tenant\""
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;SslMode=Require;Search Path=\"Tenant\""
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "\"Tenant\".Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to clear destination table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSamePostgreSqlTargetWhenDatabaseDefaultsToUsernameBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Username=app;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=app;Username=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("public.users", "public.users", new[] { "id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationPostgreSqlUserSchemaBeforePublic()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=app;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "app.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("omits Search Path", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationPostgreSqlPublicFallbackWhenSearchPathOmitted()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=app;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "public.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("omits Search Path", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationPostgreSqlUserSearchPathBeforeFallback()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=app;Password=one;Search Path=$user, public"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path=$user, public"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "public.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("omits Search Path", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationPostgreSqlSingleQuotedUserSearchPathBeforeFallback()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=app;Password=one;Search Path='$user, public'"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path='$user, public'"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "public.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("omits Search Path", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksClearDestinationPostgreSqlFallbackSchemaSearchPath()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=app;Password=one;Search Path=tenant, public"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path=tenant, public"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "public.Rows", new[] { "id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("omits Search Path", exception.Message);
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_BlocksSameMySqlCurrentDatabaseQualifiedTableBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=app;User ID=reader;Password=one"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.MySql,
                ConnectionString = "Server=localhost;Database=app;User ID=writer;Password=two"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Rows", "app.Rows", new[] { "Id" })
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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
    public void NormalizeTableName_PostgreSqlTreatsPublicSchemaAsDefault()
    {
        var unqualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "users");
        var publicQualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "public.users");
        var quotedPublicQualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "\"public\".users");
        var quotedMixedPublicQualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "\"Public\".users");

        Assert.Equal(unqualified, publicQualified);
        Assert.Equal(unqualified, quotedPublicQualified);
        Assert.NotEqual(unqualified, quotedMixedPublicQualified);
    }

    [Fact]
    public void NormalizeTableName_PostgreSqlUsesDefaultSchemaWhenProvided()
    {
        var appDefault = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "users", null, "app");
        var appQualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "app.users", null, "app");
        var publicQualified = InvokeNormalizeTableName(DbaTableCopyProvider.PostgreSql, "public.users", null, "app");

        Assert.Equal(appDefault, appQualified);
        Assert.NotEqual(appDefault, publicQualified);
    }

    [Fact]
    public void NormalizeTableName_MySqlFoldsTableNameCaseForSafety()
    {
        var rows = InvokeNormalizeTableName(DbaTableCopyProvider.MySql, "Rows", "app");
        var lowerRows = InvokeNormalizeTableName(DbaTableCopyProvider.MySql, "rows", "app");
        var currentDatabaseQualified = InvokeNormalizeTableName(DbaTableCopyProvider.MySql, "app.Rows", "app");

        Assert.Equal(rows, currentDatabaseQualified);
        Assert.Equal(rows, lowerRows);
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
        var lowerCaseTable = InvokeNormalizeTableName(DbaTableCopyProvider.SqlServer, "rows", "tempdb");

        Assert.Equal(onePart, currentDatabaseQualified);
        Assert.NotEqual(onePart, otherDatabaseQualified);
        Assert.Equal(onePart, lowerCaseTable);
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

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
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

        if (InvokeUsesCaseInsensitivePaths(Path.GetTempPath()))
        {
            Assert.Equal(firstIdentity, secondIdentity);
        }
        else
        {
            Assert.NotEqual(firstIdentity, secondIdentity);
        }
    }

    [Fact]
    public void TryCreate_SQLiteIdentityDistinguishesNamedMemoryDatabaseFromFile()
    {
        var file = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=shared"
        };
        var memory = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=shared;Mode=Memory;Cache=Shared"
        };
        var sameMemory = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=shared;Cache=Shared;Mode=Memory"
        };

        var fileIdentity = InvokeTryCreateIdentity(file);
        var memoryIdentity = InvokeTryCreateIdentity(memory);
        var sameMemoryIdentity = InvokeTryCreateIdentity(sameMemory);

        Assert.NotEqual(fileIdentity, memoryIdentity);
        Assert.Equal(memoryIdentity, sameMemoryIdentity);
    }

    [Fact]
    public void TryCreate_SQLiteIdentityAcceptsRawPathContainingEqualsSign()
    {
        var path = Path.Join(Path.GetTempPath(), "dbax=blue.db");
        var options = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = path
        };

        var identity = InvokeTryCreateIdentity(options);

        Assert.Contains("sqlite|path=", identity);
        Assert.Contains("dbax=blue.db", identity, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryCreate_SQLiteIdentityResolvesFileSymlinkToTarget()
    {
        var directory = Path.Join(Path.GetTempPath(), "dbax-symlink-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var target = Path.Join(directory, "target.db");
            var link = Path.Join(directory, "link.db");
            File.WriteAllText(target, string.Empty);
            try
            {
                File.CreateSymbolicLink(link, target);
            }
            catch (IOException)
            {
                return;
            }
            catch (NotSupportedException)
            {
                return;
            }
            catch (UnauthorizedAccessException)
            {
                return;
            }

            var targetOptions = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SQLite,
                ConnectionString = "Data Source=" + target
            };
            var linkOptions = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SQLite,
                ConnectionString = "Data Source=" + link
            };

            Assert.Equal(InvokeTryCreateIdentity(targetOptions), InvokeTryCreateIdentity(linkOptions));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void TryCreate_SQLiteIdentityCanonicalizesFullUriFilePath()
    {
        var path = Path.Join(Path.GetTempPath(), "dbax-fulluri-" + Guid.NewGuid().ToString("N") + ".db");
        var pathOptions = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "Data Source=" + path
        };
        var uriOptions = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SQLite,
            ConnectionString = "FullUri=" + new Uri(path).AbsoluteUri
        };

        Assert.Equal(InvokeTryCreateIdentity(pathOptions), InvokeTryCreateIdentity(uriOptions));
    }

    [Fact]
    public void TryCreate_PostgreSqlIdentityPreservesCaseSensitiveDatabaseNames()
    {
        var first = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.PostgreSql,
            ConnectionString = "Host=localhost;Database=App;Username=u;Password=p"
        };
        var second = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.PostgreSql,
            ConnectionString = "Host=LOCALHOST;Database=app;Username=u;Password=p"
        };

        Assert.NotEqual(InvokeTryCreateIdentity(first), InvokeTryCreateIdentity(second));
    }

    [Fact]
    public void TryCreate_MySqlIdentityFoldsCaseOnlyDatabaseNames()
    {
        var first = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.MySql,
            ConnectionString = "Server=localhost;Database=App;User ID=u;Password=p"
        };
        var second = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.MySql,
            ConnectionString = "Server=LOCALHOST;Database=app;User ID=u;Password=p"
        };

        Assert.Equal(InvokeTryCreateIdentity(first), InvokeTryCreateIdentity(second));
    }

    [Fact]
    public void TryCreate_SqlServerIdentityPreservesCaseSensitiveDatabaseNames()
    {
        var first = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SqlServer,
            ConnectionString = "Server=.;Database=App;Integrated Security=True"
        };
        var second = new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SqlServer,
            ConnectionString = "Server=.;Database=app;Integrated Security=True"
        };

        Assert.NotEqual(InvokeTryCreateIdentity(first), InvokeTryCreateIdentity(second));
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

            var result = await CreateRunner().CopyAsync(new DbaProviderTableCopyRequest
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

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
            Assert.Contains("also used as a source table", exception.Message);
        }
        finally
        {
            DeleteIfExists(databasePath);
        }
    }

    [Fact]
    public async Task CopyAsync_BlocksProviderFoldedDuplicateClearDestinationsBeforeConnecting()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=reader;Password=one;Search Path=public"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.PostgreSql,
                ConnectionString = "Host=localhost;Database=Monitoring;Username=writer;Password=two;Search Path=public"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("SourceRows", "Rows", new[] { "Id" }),
                new DbaTableCopyDefinition("OtherRows", "rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            },
            AllowSameProviderTableCopy = true
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("multiple definitions targeting destination", exception.Message);
    }

    [Fact]
    public async Task CopyAsync_RejectsUnqualifiedSqlServerDestinationClearWhenDefaultSchemaIsUnknown()
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
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=tempdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("SourceRows", "Rows", new[] { "Id" }),
                new DbaTableCopyDefinition("OtherRows", "app.Rows", new[] { "Id" })
            },
            Options = new DbaTableCopyOptions
            {
                ClearDestination = true
            }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("unqualified", exception.Message);
        Assert.Contains("default schema is unknown", exception.Message);
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
        => InvokeNormalizeTableName(provider, tableName, currentDatabase, null);

    private static string InvokeNormalizeTableName(DbaTableCopyProvider provider, string tableName, string? currentDatabase, string? defaultSchema)
    {
        var type = typeof(DbaProviderTableCopyRunner).Assembly.GetType("DBAClientX.DataMovement.DbaProviderTableCopyTargetIdentity")
            ?? throw new InvalidOperationException("DbaProviderTableCopyTargetIdentity type was not found.");
        var method = type.GetMethod(
                "NormalizeTableName",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic,
                null,
                new[] { typeof(DbaTableCopyProvider), typeof(string), typeof(string), typeof(string) },
                null)
            ?? throw new MissingMethodException(type.FullName, "NormalizeTableName");

        return (string)method.Invoke(null, new object?[] { provider, tableName, currentDatabase, defaultSchema })!;
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

    private static bool InvokeUsesCaseInsensitivePaths(string path)
    {
        var type = typeof(DbaProviderTableCopyRunner).Assembly.GetType("DBAClientX.DataMovement.DbaProviderTableCopyTargetIdentity")
            ?? throw new InvalidOperationException("DbaProviderTableCopyTargetIdentity type was not found.");
        var method = type.GetMethod("UsesCaseInsensitivePaths", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
            ?? throw new MissingMethodException(type.FullName, "UsesCaseInsensitivePaths");

        return (bool)method.Invoke(null, new object?[] { path })!;
    }

    private static DbaProviderTableCopyRunner CreateRunner()
        => new(CreateAdapter, CreateAdapter);

    private static DbaProviderTableCopyAdapterBase CreateAdapter(DbaProviderTableCopyAdapterOptions options)
        => options.Provider switch
        {
            DbaTableCopyProvider.SqlServer => new SqlServerTableCopyAdapter(options),
            DbaTableCopyProvider.PostgreSql => new PostgreSqlTableCopyAdapter(options),
            DbaTableCopyProvider.MySql => new MySqlTableCopyAdapter(options),
            DbaTableCopyProvider.Oracle => new OracleTableCopyAdapter(options),
            DbaTableCopyProvider.SQLite => new SQLiteTableCopyAdapter(options),
            _ => throw new NotSupportedException($"Provider '{options.Provider}' is not supported.")
        };

    private static void InvokeValidateSameProviderTableCopy(DbaProviderTableCopyRequest request)
    {
        var method = typeof(DbaProviderTableCopyRunner).GetMethod("ValidateSameProviderTableCopy", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyRunner), "ValidateSameProviderTableCopy");
        try
        {
            method.Invoke(null, new object?[] { request });
        }
        catch (System.Reflection.TargetInvocationException exception) when (exception.InnerException != null)
        {
            throw exception.InnerException;
        }
    }

    private static void InvokeValidateDestinationBulkCopyRequirements(DbaProviderTableCopyRequest request)
    {
        var method = typeof(DbaProviderTableCopyRunner).GetMethod("ValidateDestinationBulkCopyRequirements", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyRunner), "ValidateDestinationBulkCopyRequirements");
        try
        {
            method.Invoke(null, new object?[] { request });
        }
        catch (System.Reflection.TargetInvocationException exception) when (exception.InnerException != null)
        {
            throw exception.InnerException;
        }
    }
}
