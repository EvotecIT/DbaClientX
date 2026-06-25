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
        var sourcePath = Path.Combine(Path.GetTempPath(), "DbaClientX-source-" + Guid.NewGuid().ToString("N") + ".db");
        var destinationPath = Path.Combine(Path.GetTempPath(), "DbaClientX-destination-" + Guid.NewGuid().ToString("N") + ".db");
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
    public async Task CopyAsync_DeduplicatesSQLiteSourceRowsByCaseInsensitiveKey()
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
        var sourcePath = Path.Combine(Path.GetTempPath(), "DbaClientX-source-" + Guid.NewGuid().ToString("N") + ".db");
        var destinationPath = Path.Combine(Path.GetTempPath(), "DbaClientX-destination-" + Guid.NewGuid().ToString("N") + ".db");
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
        var sourcePath = Path.Combine(Path.GetTempPath(), "DbaClientX-source-" + Guid.NewGuid().ToString("N") + ".db");
        var destinationPath = Path.Combine(Path.GetTempPath(), "DbaClientX-destination-" + Guid.NewGuid().ToString("N") + ".db");
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
}
