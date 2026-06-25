using System.Data;
using System.Reflection;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class DbaProviderTableCopyAdapterTests
{
    [Fact]
    public async Task CopyAsync_CopiesRowsBetweenSQLiteConnectionStrings()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One'), (2, 'Two'), (3, 'Three');");
            }

            var source = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "Id" });
            var destination = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) },
                new DbaTableCopyOptions { PageSize = 2 });

            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
            Assert.Equal(3, result.SourceRows);
            Assert.Equal(3, result.DestinationRows);

            using (var sqlite = new SQLite())
            {
                var count = sqlite.ExecuteScalar(destinationPath, "SELECT COUNT(*) FROM DestinationRows;");
                Assert.Equal(3L, Convert.ToInt64(count));
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_SQLiteBulkWritePreservesDotsInsideQuotedDestinationSegments()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE \"Rows.Source\" (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE \"Rows.Current\" (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO \"Rows.Source\" (Id, DisplayName) VALUES (1, 'One'), (2, 'Two');");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO \"Rows.Current\" (Id, DisplayName) VALUES (99, 'Old');");
            }

            var source = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "Id" });
            var destination = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("\"Rows.Source\"", "\"Rows.Current\"", new[] { "Id" }) },
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var rows = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT Id, DisplayName FROM \"Rows.Current\" ORDER BY Id;"));
                Assert.Equal(2, rows.Rows.Count);
                Assert.Equal("One", rows.Rows[0]["DisplayName"]);
                Assert.Equal("Two", rows.Rows[1]["DisplayName"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_DeduplicatesSQLiteSourceRowsByCaseInsensitiveKey()
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

            var source = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "ProbeName" });
            var destination = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[]
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
                new DbaTableCopyOptions { PageSize = 1 });

            Assert.True(result.Verified);
            Assert.Equal(2, result.SourceRows);
            Assert.Equal(2, result.CopiedRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var rows = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT ProbeName, LastCompletedUtcMs, StatusId FROM ProbeIndex ORDER BY lower(ProbeName);"));
                Assert.Equal(2, rows.Rows.Count);
                Assert.Equal("server1", rows.Rows[0]["ProbeName"]);
                Assert.Equal(20L, Convert.ToInt64(rows.Rows[0]["LastCompletedUtcMs"]));
                Assert.Equal(2L, Convert.ToInt64(rows.Rows[0]["StatusId"]));
                Assert.Equal("Server2", rows.Rows[1]["ProbeName"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_TreatsMissingSQLiteSourceTableAsEmptyWhenConfigured()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE ExistingRows (Id INTEGER NOT NULL PRIMARY KEY);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE MissingRows (Id INTEGER NOT NULL PRIMARY KEY);");
            }

            var source = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                treatMissingTablesAsEmpty: true);
            var destination = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath,
                treatMissingTablesAsEmpty: true);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("MissingRows", "MissingRows", new[] { "Id" }) });

            Assert.True(result.Verified);
            Assert.Equal(0, result.SourceRows);
            Assert.Equal(0, result.CopiedRows);
            Assert.Equal(0, result.DestinationRows);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_ClearsDependentSQLiteTablesInReverseDefinitionOrder()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                CreateHistoryTables(sqlite, sourcePath);
                CreateHistoryTables(sqlite, destinationPath);
                sqlite.ExecuteNonQuery(
                    destinationPath,
                    """
                    CREATE TRIGGER BlockProbeResultDeleteBeforeMetadata
                    BEFORE DELETE ON ProbeResults
                    WHEN EXISTS (SELECT 1 FROM ProbeResultMetadata WHERE ResultId = OLD.ResultId)
                    BEGIN
                        SELECT RAISE(ABORT, 'metadata exists');
                    END;
                    """);
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeResults (ResultId, ProbeName, IsMaintenance) VALUES (1, 'Dns', 1);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeResultMetadata (ResultId, MetaKey, MetaValue) VALUES (1, 'Zone', 'contoso.com');");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO ProbeResults (ResultId, ProbeName, IsMaintenance) VALUES (99, 'Old', 0);");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO ProbeResultMetadata (ResultId, MetaKey, MetaValue) VALUES (99, 'OldKey', 'OldValue');");
            }

            var source = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "ResultId" });
            var destination = new DbaProviderTableCopyAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[]
                {
                    new DbaTableCopyDefinition("ProbeResults", "ProbeResults", new[] { "ResultId" }),
                    new DbaTableCopyDefinition("ProbeResultMetadata", "ProbeResultMetadata", new[] { "ResultId", "MetaKey" })
                },
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.SourceRows);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2, result.DestinationRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var metadata = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT ResultId, MetaKey, MetaValue FROM ProbeResultMetadata;"));
                var row = Assert.Single(metadata.Rows.Cast<DataRow>());
                Assert.Equal(1L, Convert.ToInt64(row["ResultId"]));
                Assert.Equal("Zone", row["MetaKey"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public void BuildPageQuery_OracleFoldsSimpleIdentifiersToUppercase()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Id" });

        var query = InvokeBuildPageQuery(adapter, "app.Users", 0, 10);

        Assert.Equal("SELECT * FROM APP.USERS ORDER BY ID OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesNonSimpleIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Created At" });

        var query = InvokeBuildPageQuery(adapter, "app.User Audit", 5, 10);

        Assert.Equal("SELECT * FROM APP.\"User Audit\" ORDER BY \"Created At\" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesIdentifiersStartingWithUnderscore()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "_SortKey" });

        var query = InvokeBuildPageQuery(adapter, "app._Audit", 0, 10);

        Assert.Equal("SELECT * FROM APP.\"_Audit\" ORDER BY \"_SortKey\" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleAllowUnorderedDoesNotOrderByFirstColumn()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            allowUnordered: true);

        var query = InvokeBuildPageQuery(adapter, "app.Users", 0, 10);

        Assert.Equal("SELECT * FROM APP.USERS OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlFoldsSimpleIdentifiersToLowercase()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "CreatedUtc" });

        var query = InvokeBuildPageQuery(adapter, "Public.Users", 0, 10);

        Assert.Equal("SELECT * FROM public.users ORDER BY createdutc LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlPreservesExplicitQuotedIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "\"CreatedUtc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"Public\".\"Users\"", 0, 10);

        Assert.Equal("SELECT * FROM \"Public\".\"Users\" ORDER BY \"CreatedUtc\" LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlPreservesDotsInsideExplicitQuotedIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "\"Created.Utc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"Tenant.v1\".\"Users.Current\"", 0, 10);

        Assert.Equal("SELECT * FROM \"Tenant.v1\".\"Users.Current\" ORDER BY \"Created.Utc\" LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_SqlServerPreservesDotsInsideBracketedIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "[Created.Utc]" });

        var query = InvokeBuildPageQuery(adapter, "[tenant.v1].[Users.Current]", 0, 10);

        Assert.Equal("SELECT * FROM [tenant.v1].[Users.Current] ORDER BY [Created.Utc] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OraclePreservesExplicitQuotedIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "\"CreatedUtc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"App\".\"Users\"", 0, 10);

        Assert.Equal("SELECT * FROM \"App\".\"Users\" ORDER BY \"CreatedUtc\" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BulkDestinationName_PostgreSqlFoldsSimpleIdentifiersBeforeProviderBulkCopyQuotesThem()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");

        var normalized = InvokeNormalizePostgreSqlBulkDestinationTableName(adapter, "Public.Users");

        Assert.Equal("public.users", normalized);
    }

    [Fact]
    public void BulkDestinationName_PostgreSqlPreservesDotsInsideExplicitQuotedSegments()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");

        var normalized = InvokeNormalizePostgreSqlBulkDestinationTableName(adapter, "\"tenant.v1\".\"Rows.Current\"");

        Assert.Equal("\"tenant.v1\".\"Rows.Current\"", normalized);
    }

    [Theory]
    [InlineData("[Rows.Current]", "\"Rows.Current\"")]
    [InlineData("`Rows.Current`", "\"Rows.Current\"")]
    public void BulkDestinationName_SQLiteStripsAlternativeIdentifierDelimitersBeforeQuoting(string destinationTableName, string expected)
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.SQLite,
            "Data Source=:memory:");

        var normalized = InvokeNormalizeSQLiteBulkDestinationTableName(adapter, destinationTableName);

        Assert.Equal(expected, normalized);
    }

    [Fact]
    public void BulkPage_PostgreSqlNormalizesSimpleColumnNamesBeforeProviderBulkCopyQuotesThem()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));
        page.Columns.Add("Created At", typeof(string));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "Users");

        Assert.Equal(new[] { "displayname", "CreatedUtc", "Created At" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Equal("DisplayName", page.Columns[0].ColumnName);
    }

    [Fact]
    public void BulkPage_PostgreSqlPreservesSimpleColumnCaseForQuotedDestinationTable()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "\"Public\".\"Users\"");

        Assert.Equal(new[] { "DisplayName", "CreatedUtc" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public void BulkPage_PostgreSqlFoldsSimpleColumnNamesWhenOnlySchemaIsQuoted()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "\"TenantA\".Users");

        Assert.Equal(new[] { "displayname", "CreatedUtc" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public void ValidatePage_PostgreSqlRejectsDuplicateNormalizedColumnsBeforeWrite()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("displayname", typeof(string));

        var exception = Assert.Throws<InvalidOperationException>(() => adapter.ValidatePage(new DbaTableCopyDefinition("Users", "Users"), page));

        Assert.Contains("duplicate destination column 'displayname'", exception.Message);
    }

    [Theory]
    [InlineData("relation \"missing\" does not exist", true)]
    [InlineData("no such table: MissingRows", true)]
    [InlineData("invalid object name 'dbo.MissingRows'.", true)]
    [InlineData("function lower(integer) does not exist", false)]
    [InlineData("column \"BadKey\" does not exist", false)]
    [InlineData("no such column: BadKey", false)]
    [InlineData("Unknown column 'BadKey' in 'field list'", false)]
    [InlineData("Invalid column name 'BadKey'.", false)]
    public void MissingTableDetection_DoesNotTreatMissingColumnsAsMissingTables(string message, bool expected)
    {
        var actual = InvokeIsMissingTableException(new InvalidOperationException(message));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void BuildPageQuery_SqlServerStripsDelimitersBeforeQuotingIdentifiers()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "[Id]" });

        var query = InvokeBuildPageQuery(adapter, "[dbo].[Rows]", 0, 10);

        Assert.Equal("SELECT * FROM [dbo].[Rows] ORDER BY [Id] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_DeduplicationUsesNamespacedRankAlias()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "ProbeName" });

        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "BuildPageQuery");
        var query = (string)method.Invoke(
            adapter,
            new object?[]
            {
                "Public.ProbeIndex",
                new[] { "ProbeName" },
                new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtc" }),
                0L,
                10
            })!;

        Assert.Contains("__DbaXCRank_62D977CD", query);
        Assert.DoesNotContain("__DbaXRank", query);
    }

    [Fact]
    public void BuildPageQuery_OracleDeduplicationRankAliasFitsLegacyIdentifierLimit()
    {
        const string rankAlias = "__DbaXCRank_62D977CD";
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "ProbeName" });

        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "BuildPageQuery");
        var query = (string)method.Invoke(
            adapter,
            new object?[]
            {
                "App.ProbeIndex",
                new[] { "ProbeName" },
                new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtc" }),
                0L,
                10
            })!;

        Assert.True(rankAlias.Length <= 30);
        Assert.Contains($"\"{rankAlias}\"", query);
        Assert.DoesNotContain("__DbaXCopyRank_62D977CD8E7A4BC08D1A73B5197F33D4", query);
    }

    [Fact]
    public async Task CopyAsync_SqlServerSameTableProtectionTreatsUnqualifiedNameAsDbo()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=tempdb;Integrated Security=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=tempdb;Integrated Security=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("Users", "[dbo].[Users]", new[] { "Id" })
            }
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaProviderTableCopyRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", ex.Message);
    }

    [Fact]
    public void BuildPageQuery_UsesDefinitionOrderColumnsWhenProvided()
    {
        var adapter = new DbaProviderTableCopyAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "FallbackId" });

        var query = InvokeBuildPageQuery(adapter, "dbo.Users", 0, 10, new[] { "DefinitionId" });

        Assert.Equal("SELECT * FROM [dbo].[Users] ORDER BY [DefinitionId] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    private static string InvokeBuildPageQuery(DbaProviderTableCopyAdapter adapter, string tableName, long offset, int pageSize, IReadOnlyList<string>? orderByColumns = null)
    {
        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "BuildPageQuery");

        return (string)method.Invoke(adapter, new object?[] { tableName, orderByColumns, null, offset, pageSize })!;
    }

    private static string InvokeNormalizePostgreSqlBulkDestinationTableName(DbaProviderTableCopyAdapter adapter, string destinationTableName)
    {
        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("NormalizePostgreSqlBulkDestinationTableName", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "NormalizePostgreSqlBulkDestinationTableName");

        return (string)method.Invoke(adapter, new object?[] { destinationTableName })!;
    }

    private static string InvokeNormalizeSQLiteBulkDestinationTableName(DbaProviderTableCopyAdapter adapter, string destinationTableName)
    {
        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("NormalizeSQLiteBulkDestinationTableName", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "NormalizeSQLiteBulkDestinationTableName");

        return (string)method.Invoke(adapter, new object?[] { destinationTableName })!;
    }

    private static DataTable InvokeNormalizePostgreSqlBulkPage(DbaProviderTableCopyAdapter adapter, DataTable page, string destinationTableName)
    {
        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("NormalizePostgreSqlBulkPage", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "NormalizePostgreSqlBulkPage");

        return (DataTable)method.Invoke(adapter, new object?[] { page, destinationTableName })!;
    }

    private static bool InvokeIsMissingTableException(Exception exception)
    {
        var method = typeof(DbaProviderTableCopyAdapter).GetMethod("IsMissingTableException", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapter), "IsMissingTableException");

        return (bool)method.Invoke(null, new object?[] { exception })!;
    }

    private static void CreateHistoryTables(SQLite sqlite, string path)
    {
        sqlite.ExecuteNonQuery(path, "CREATE TABLE ProbeResults (ResultId INTEGER NOT NULL PRIMARY KEY, ProbeName TEXT NOT NULL, IsMaintenance INTEGER NOT NULL);");
        sqlite.ExecuteNonQuery(path, "CREATE TABLE ProbeResultMetadata (ResultId INTEGER NOT NULL, MetaKey TEXT NOT NULL, MetaValue TEXT NOT NULL, PRIMARY KEY (ResultId, MetaKey));");
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
