using System.Data;
using System.Data.Common;
using DBAClientX;
using DBAClientX.DataMovement;
using MySqlConnector;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyLiveProviderReliabilityTests
{
    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION", false)]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION", true)]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION", false)]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION", true)]
    [Trait("Category", "LiveProvider")]
    public async Task ClearDestination_PreflightsRelatedTablesInOneTransaction(
        DbaTableCopyProvider provider,
        string environmentVariable,
        bool verifyContent)
    {
        var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceParent = "dbax_sp_" + suffix;
        string sourceChild = "dbax_sc_" + suffix;
        string destinationParent = "dbax_dp_" + suffix;
        string destinationChild = "dbax_dc_" + suffix;
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        string Quote(string name) => provider == DbaTableCopyProvider.PostgreSql ? $"\"{name}\"" : $"`{name}`";
        string engine = provider == DbaTableCopyProvider.MySql ? " ENGINE=InnoDB" : string.Empty;
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(sourceParent)} (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(sourceChild)} (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(destinationParent)} (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(destinationChild)} (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, CONSTRAINT {Quote("fk_" + suffix)} FOREIGN KEY (parent_id) REFERENCES {Quote(destinationParent)} (id)){engine}");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(sourceParent)} VALUES (1, 'first parent'), (2, 'new parent')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(sourceChild)} VALUES (20, 2, 'new child')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(destinationParent)} VALUES (1, 'old parent')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(destinationChild)} VALUES (10, 1, 'old child')");

            var source = CreateAdapter(provider, connectionString!, new[] { "id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definitions = new[]
            {
                new DbaTableCopyDefinition(sourceParent, destinationParent, new[] { "id" }) { UseKeysetPagination = true },
                new DbaTableCopyDefinition(sourceChild, destinationChild, new[] { "id" }) { UseKeysetPagination = true }
            };
            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                definitions,
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    VerifyContent = verifyContent,
                    PageSize = 1
                });

            Assert.Equal(3, result.CopiedRows);
            Assert.True(result.Verified);
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT id FROM {Quote(destinationParent)}")));
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT parent_id FROM {Quote(destinationChild)}")));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationChild));
            await TryExecuteAsync(connection, DropTableSql(provider, destinationParent));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceChild));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceParent));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointIdentity_DistinguishesCaseOnlyTablesOnCaseSensitiveServers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        int lowerCaseTableNames = Convert.ToInt32(await ExecuteScalarAsync(connection, "SELECT @@lower_case_table_names"));
        Assert.SkipWhen(lowerCaseTableNames != 0, "This contract requires a case-sensitive MySQL table-name server.");

        string lower = "dbaxcase_" + Guid.NewGuid().ToString("N").Substring(0, 12);
        string upper = lower.ToUpperInvariant();
        string suffix = Guid.NewGuid().ToString("N");
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{lower}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"CREATE TABLE `{upper}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            var adapter = new MySqlTableCopyAdapter(connectionString!);
            var lowerDefinition = new DbaTableCopyDefinition("unused", lower);
            var upperDefinition = new DbaTableCopyDefinition("unused", upper);
            var lowerCheckpoint = CreateInitialCheckpoint("case-lower-" + suffix);
            var upperCheckpoint = CreateInitialCheckpoint("case-upper-" + suffix);

            await adapter.InitializeCheckpointAsync(lowerDefinition, lowerCheckpoint, clearDestination: false);
            await adapter.InitializeCheckpointAsync(upperDefinition, upperCheckpoint, clearDestination: false);

            Assert.Equal(lowerCheckpoint, await adapter.ReadCheckpointAsync(lowerDefinition));
            Assert.Equal(upperCheckpoint, await adapter.ReadCheckpointAsync(upperDefinition));
        }
        finally
        {
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{upper}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{lower}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlBulkCopy_WritesProviderNeutralYearMonthIntervalsLosslessly()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_interval_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{table}\" (id BIGINT NOT NULL PRIMARY KEY, period INTERVAL NOT NULL)");
            using var page = new DataTable(table);
            page.Columns.Add("id", typeof(long));
            page.Columns.Add("period", typeof(DbaYearMonthInterval));
            page.Rows.Add(1L, new DbaYearMonthInterval(-27));

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", table, new[] { "id" });
            await destination.WritePageAsync(definition, page, new DbaTableCopyOptions());

            Assert.Equal(-27L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT EXTRACT(YEAR FROM period)::bigint * 12 + EXTRACT(MONTH FROM period)::bigint FROM \"{table}\" WHERE id = 1")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_RejectsNontransactionalDestinationBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_myisam_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=MyISAM");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (1, 'preserve')");

            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "myisam-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("InnoDB", exception.Message, StringComparison.Ordinal);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointStorage_RejectsMissingSelectedDatabaseBeforeChangingQualifiedDestination()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var configured = new MySqlConnectionStringBuilder(connectionString!);
        Assert.False(string.IsNullOrWhiteSpace(configured.Database));
        string database = configured.Database;
        var table = "dbax_no_database_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{table}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{table}` VALUES (1, 'preserve')");

            configured.Database = string.Empty;
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, configured.ConnectionString);
            var definition = new DbaTableCopyDefinition("unused", $"`{database}`.`{table}`", new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "no-database-" + table,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("selected database", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{table}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{table}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlConsistentRead_RejectsNontransactionalSourceBeforeCopyingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_myisam_source_" + suffix;
        var destinationTable = "dbax_innodb_destination_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=MyISAM");
            await ExecuteAsync(connection, $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{sourceTable}` VALUES (1, 'source')");

            var source = CreateAdapter(
                DbaTableCopyProvider.MySql,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("InnoDB", exception.Message, StringComparison.Ordinal);
            Assert.Equal(0L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlConsistentRead_ToleratesMissingSourceAsEmpty()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var source = new MySqlTableCopyAdapter(
            connectionString!,
            new[] { "id" },
            treatMissingTablesAsEmpty: true)
        {
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot
        };
        var definition = new DbaTableCopyDefinition(
            "dbax_missing_" + Guid.NewGuid().ToString("N"),
            "unused",
            new[] { "id" })
        {
            UseKeysetPagination = true
        };

        using var session = await source.OpenReadSessionAsync(new[] { definition });
        using var page = await source.ReadPageAsync(new(definition, null, 1));

        Assert.Empty(page.Data.Rows.Cast<DataRow>());
        Assert.Null(page.ContinuationToken);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlConsistentRead_ToleratesMissingSourceWithoutAbortingTransaction()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var source = new PostgreSqlTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.PostgreSql,
            ConnectionString = connectionString!,
            DefaultOrderByColumns = new[] { "id" },
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot,
            TreatMissingTablesAsEmpty = true
        });
        var definition = new DbaTableCopyDefinition(
            "dbax_missing_" + Guid.NewGuid().ToString("N"),
            "unused",
            new[] { "id" })
        {
            UseKeysetPagination = true
        };

        using var session = await source.OpenReadSessionAsync(new[] { definition });
        using var page = await source.ReadPageAsync(new(definition, null, 1));

        Assert.Empty(page.Data.Rows.Cast<DataRow>());
        Assert.Null(page.ContinuationToken);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_UsesMappedNamesWhenPhysicalColumnOrderDiffers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_mapping_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-mapping-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (destination_payload VARCHAR(64) NOT NULL, destination_id BIGINT NOT NULL PRIMARY KEY)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (SourceId INTEGER NOT NULL PRIMARY KEY, SourcePayload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'One'), (2, 'Dwa')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "SourceId" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                destinationTable,
                new[] { "SourceId" },
                ColumnMappings: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["SourceId"] = "destination_id",
                    ["SourcePayload"] = "destination_payload"
                })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    CheckpointId = "mapping-" + suffix,
                    PageSize = 1,
                    VerifyContent = true
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(
                "1:One",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(destination_id, ':', destination_payload) FROM `{destinationTable}` ORDER BY destination_id LIMIT 1")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_PreflightsRequiredDestinationColumnsBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_schema_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-schema-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, required_value INT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (99, 'preserve', 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "schema-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("required_value", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', payload, ':', required_value) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_PreflightsRequiredDestinationColumnsBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_schema_" + suffix;
        var firstSearchPathSchema = "dbax_empty_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-schema-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA \"{firstSearchPathSchema}\"");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE public.\"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL, required_value integer NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO public.\"{destinationTable}\" VALUES (99, 'preserve', 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destinationConnection = new NpgsqlConnectionStringBuilder(connectionString!)
            {
                SearchPath = $"{firstSearchPathSchema},public"
            }.ConnectionString;
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, destinationConnection);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "schema-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("required_value", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload || ':' || required_value FROM public.\"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS public.\"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP SCHEMA IF EXISTS \"{firstSearchPathSchema}\" CASCADE");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_PreflightsIncompatibleTypesBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_types_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-types-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, required_value integer NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, required_value TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'not-an-integer')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "types-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("CLR types", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || required_value FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_PreflightsIncompatibleTypesBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_types_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-mysql-types-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL PRIMARY KEY, required_value integer NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, required_value TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'not-an-integer')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "types-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', required_value) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Theory]
    [Trait("Category", "LiveProvider")]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    public async Task CheckpointedCopy_PreflightsActualDestinationConstraintsBeforeClearingRows(
        DbaTableCopyProvider provider)
    {
        string environmentVariable = provider == DbaTableCopyProvider.PostgreSql
            ? "DBACLIENTX_POSTGRESQL_TEST_CONNECTION"
            : "DBACLIENTX_MYSQL_TEST_CONNECTION";
        string? connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string destinationTable = "dbax_constraints_" + suffix;
        string sqlitePath = Path.Combine(Path.GetTempPath(), $"dbax-{provider}-constraints-{suffix}.sqlite");
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            string createSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, constrained_value bigint NOT NULL CHECK (constrained_value > 0))"
                : $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL PRIMARY KEY, constrained_value bigint NOT NULL CHECK (constrained_value > 0)) ENGINE=InnoDB";
            await ExecuteAsync(connection, createSql);
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)"
                    : $"INSERT INTO `{destinationTable}` VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, constrained_value INTEGER NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 1), (2, -1)");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "constraints-" + suffix,
                        ClearDestination = true,
                        PageSize = 2
                    }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            string preservedValueSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT id || ':' || constrained_value FROM \"{destinationTable}\""
                : $"SELECT CONCAT(id, ':', constrained_value) FROM `{destinationTable}`";
            Assert.Equal("99:7", Convert.ToString(await ExecuteScalarAsync(connection, preservedValueSql)));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DeleteCheckpointSql(provider, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointStorage_RejectsUnloggedTableBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string schema = "dbax_checkpoint_" + suffix;
        string destinationTable = "destination_rows";
        var builder = new NpgsqlConnectionStringBuilder(connectionString!) { SearchPath = schema };
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA \"{schema}\"");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{schema}\".\"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{schema}\".\"{destinationTable}\" VALUES (99, 'preserve')");
            await ExecuteAsync(
                connection,
                $"CREATE UNLOGGED TABLE \"{schema}\".\"DbaClientX_TableCopyCheckpoints\" (id integer)");

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, builder.ConnectionString);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "checkpoint-storage-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("permanent logged table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{schema}\".\"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_RejectsViewBeforeClearingUnderlyingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var baseTable = "dbax_view_base_" + suffix;
        var destinationView = "dbax_view_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-view-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{baseTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{baseTable}\" VALUES (99, 'preserve')");
            await ExecuteAsync(
                connection,
                $"CREATE VIEW \"{destinationView}\" AS SELECT id, payload FROM \"{baseTable}\"");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationView, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "view-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("cannot be resolved to a PostgreSQL table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{baseTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP VIEW IF EXISTS \"{destinationView}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{baseTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_RejectsUnloggedDestinationBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_unlogged_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE UNLOGGED TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserve')");

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "unlogged-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("cannot be resolved to a PostgreSQL table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlKeysetRead_RoundTripsUnsignedBigIntBeyondInt64Range()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var table = "dbax_unsigned_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{table}` (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO `{table}` VALUES (9223372036854775808, 'first'), (18446744073709551615, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "id" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));

            Assert.Equal(9223372036854775808UL, Assert.IsType<ulong>(first.Data.Rows[0]["id"]));
            Assert.Equal(ulong.MaxValue, Assert.IsType<ulong>(second.Data.Rows[0]["id"]));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{table}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlVerifiedCopy_RoundTripsArbitraryPrecisionDecimalKeysets()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_decimal_source_" + suffix;
        string destinationTable = "dbax_decimal_destination_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"CREATE TABLE `{destinationTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(
                connection,
                $"INSERT INTO `{sourceTable}` VALUES (1000000000000000000000000000000, 'first'), (10000000000000000000000000000000, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "amount" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "amount" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    VerifyContent = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(
                "1000000000000000000000000000000,10000000000000000000000000000000",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT GROUP_CONCAT(CAST(amount AS CHAR) ORDER BY amount SEPARATOR ',') FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlArbitraryPrecisionDecimal_RejectsUnsupportedDestinationBeforeCopying()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        string sourceTable = "dbax_decimal_portability_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            var source = new MySqlTableCopyAdapter(connectionString!, new[] { "amount" });
            var definition = new DbaTableCopyDefinition(sourceTable, "Destination", new[] { "amount" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                source.ValidateDestinationCompatibilityAsync(
                    DbaTableCopyProvider.SQLite,
                    new[] { definition },
                    CancellationToken.None));

            Assert.Contains("not portable to SQLite", exception.Message, StringComparison.Ordinal);
            Assert.Contains("convert it explicitly to String", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCopy_RejectsOversizedNumericShapeBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_numeric_source_" + suffix;
        string destinationTable = "dbax_numeric_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, amount numeric(65,0) NOT NULL)");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, amount numeric(65,0) NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, 1), (2, 1000000000000000000000000000000)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("System.Decimal precision", exception.Message, StringComparison.Ordinal);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || amount FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCopy_PreflightsCrossPageConstraintsBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_constraint_source_" + suffix;
        string destinationTable = "dbax_constraint_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, code text NOT NULL)");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, code text NOT NULL UNIQUE)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, 'duplicate'), (2, 'duplicate')");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || code FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [Trait("Category", "LiveProvider")]
    public async Task ClearDestination_RejectsOmittedGeneratorsWithoutAdvancingThem(DbaTableCopyProvider provider)
    {
        string environmentVariable = provider == DbaTableCopyProvider.PostgreSql
            ? "DBACLIENTX_POSTGRESQL_TEST_CONNECTION"
            : "DBACLIENTX_MYSQL_TEST_CONNECTION";
        string? connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_gs_" + suffix;
        string destinationTable = "dbax_gd_" + suffix;
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            string createSource = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{sourceTable}\" (payload text NOT NULL PRIMARY KEY)"
                : $"CREATE TABLE `{sourceTable}` (payload varchar(50) NOT NULL PRIMARY KEY) ENGINE=InnoDB";
            string createDestination = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{destinationTable}\" (id bigserial NOT NULL PRIMARY KEY, payload text NOT NULL)"
                : $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB";
            await ExecuteAsync(connection, createSource);
            await ExecuteAsync(connection, createDestination);
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{sourceTable}\" VALUES ('new')"
                    : $"INSERT INTO `{sourceTable}` VALUES ('new')");
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{destinationTable}\" (id, payload) VALUES (99, 'preserved')"
                    : $"INSERT INTO `{destinationTable}` (id, payload) VALUES (99, 'preserved')");

            string generatorStateSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT last_value::text || ':' || is_called::text FROM \"{destinationTable}_id_seq\""
                : $"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{destinationTable}'";
            string? before = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            var source = CreateAdapter(provider, connectionString!, new[] { "payload" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "payload" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(before, Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql)));
            string preservedSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT id || ':' || payload FROM \"{destinationTable}\""
                : $"SELECT CONCAT(id, ':', payload) FROM `{destinationTable}`";
            Assert.Equal("99:preserved", Convert.ToString(await ExecuteScalarAsync(connection, preservedSql)));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceTable));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlKeysetRead_RoundTripsDateAndTimeKeys()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_temporal_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{table}\" (event_date date NOT NULL, event_time time without time zone NOT NULL, payload text NOT NULL, PRIMARY KEY (event_date, event_time))");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{table}\" VALUES (DATE '2026-09-20', TIME '01:02:03.000001', 'first'), (DATE '2026-09-20', TIME '01:02:03.000002', 'second'), (DATE '2026-09-21', TIME '00:00:00', 'third')");

            var source = CreateAdapter(
                DbaTableCopyProvider.PostgreSql,
                connectionString!,
                new[] { "event_date", "event_time" });
            var definition = new DbaTableCopyDefinition(
                table,
                table,
                new[] { "event_date", "event_time" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));
            using var third = await source.ReadPageAsync(new(definition, second.ContinuationToken, 1));

            Assert.IsType<DateOnly>(first.Data.Rows[0]["event_date"]);
            Assert.IsType<TimeOnly>(first.Data.Rows[0]["event_time"]);
            Assert.Equal("first", first.Data.Rows[0].Field<string>("payload"));
            Assert.Equal("second", second.Data.Rows[0].Field<string>("payload"));
            Assert.Equal("third", third.Data.Rows[0].Field<string>("payload"));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_NormalizesNetworkValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_network_source_" + suffix;
        var destinationTable = "dbax_network_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, address inet NOT NULL, subnet cidr NOT NULL, mac macaddr NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, address inet NOT NULL, subnet cidr NOT NULL, mac macaddr NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '192.0.2.42', '198.51.100.0/24', '00:11:22:aa:bb:cc'), (2, '2001:db8::42', '2001:db8::/48', '00:11:22:aa:bb:dd')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesArrayValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_array_source_" + suffix;
        var destinationTable = "dbax_array_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, ARRAY[1,2,3]), (2, ARRAY[4,5,6])");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesRangeAndMultirangeValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_range_source_" + suffix;
        var destinationTable = "dbax_range_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, span int4range NOT NULL, spans int4multirange NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, span int4range NOT NULL, spans int4multirange NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '[1,5)'::int4range, '{{[1,5),[10,20)}}'::int4multirange), (2, 'empty'::int4range, '{{[30,40)}}'::int4multirange)");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesGeometricValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_geometry_source_" + suffix;
        var destinationTable = "dbax_geometry_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            const string columns = "id bigint NOT NULL PRIMARY KEY, p point NOT NULL, l line NOT NULL, s lseg NOT NULL, b box NOT NULL, path path NOT NULL, polygon polygon NOT NULL, circle circle NOT NULL";
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" ({columns})");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" ({columns})");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '(1,2)', '{{1,2,3}}', '[(1,2),(3,4)]', '(3,4),(1,2)', '[(1,2),(3,4)]', '((1,2),(3,4),(5,6))', '<(1,2),5>')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(1, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlKeysetRead_RoundTripsDelimitedMixedCaseKey()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_delimited_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{table}\" (\"UserId\" bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{table}\" VALUES (1, 'first'), (2, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "\"UserId\"" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "\"UserId\"" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));

            Assert.Equal(1L, first.Data.Rows[0].Field<long>("UserId"));
            Assert.Equal(2L, second.Data.Rows[0].Field<long>("UserId"));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlBoundedRead_RejectsVariableSizeNativeArraysBeforeMaterializing()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_array_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{table}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{table}\" VALUES (1, ARRAY(SELECT generate_series(1, 10000)))");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                source.ReadPageAsync(new DbaTableCopyPageRequest(definition, null, 1) { MaxBytes = 1024 }));

            Assert.Contains("variable-size native type", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Theory]
    [Trait("Category", "LiveProvider")]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION")]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION")]
    public async Task ResumableCopy_KeysetRead_AndFailedPageRollback_AreProviderBacked(
        DbaTableCopyProvider provider,
        string environmentVariable)
    {
        var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_copy_" + suffix;
        var rollbackTable = "dbax_rollback_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-live-" + suffix + ".sqlite");
        await using var connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, CreateTableSql(provider, destinationTable));
            await ExecuteAsync(connection, CreateTableSql(provider, rollbackTable));

            await VerifyAtomicRollbackAsync(provider, connectionString!, connection, rollbackTable);

            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, Payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'One'), (2, 'Dwa'), (3, '三')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "Id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                destinationTable,
                new[] { "Id" })
            {
                UseKeysetPagination = true
            };
            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    CheckpointId = "live-" + suffix,
                    PageSize = 1,
                    VerifyContent = true
                });

            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
            Assert.True((await destination.ReadCheckpointAsync(definition))!.Completed);
            if (provider == DbaTableCopyProvider.MySql)
            {
                Assert.Equal(
                    "InnoDB",
                    Convert.ToString(await ExecuteScalarAsync(
                        connection,
                        "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'DbaClientX_TableCopyCheckpoints'")));
            }

            var providerSource = CreateAdapter(
                provider,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            var providerDefinition = new DbaTableCopyDefinition(
                destinationTable,
                destinationTable,
                new[] { "id" })
            {
                UseKeysetPagination = true
            };
            using (await (providerSource is IDbaTableCopyDefinitionReadSession definitionSession
                ? definitionSession.OpenReadSessionAsync(new[] { providerDefinition })
                : ((IDbaTableCopyReadSession)providerSource).OpenReadSessionAsync()))
            {
                using var first = await providerSource.ReadPageAsync(new(providerDefinition, null, 2));
                using var second = await providerSource.ReadPageAsync(new(providerDefinition, first.ContinuationToken, 2));
                Assert.Equal(2, first.Data.Rows.Count);
                Assert.Single(second.Data.Rows.Cast<DataRow>());
            }
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DropTableSql(provider, rollbackTable));
            await TryExecuteAsync(connection, DeleteCheckpointSql(provider, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    private static async Task VerifyAtomicRollbackAsync(
        DbaTableCopyProvider provider,
        string connectionString,
        DbConnection connection,
        string table)
    {
        var destination = CreateAdapter(provider, connectionString);
        var definition = new DbaTableCopyDefinition("unused", table, new[] { "id" });
        var checkpoint = new DbaTableCopyCheckpoint
        {
            CopyId = "rollback-" + table.Substring(table.LastIndexOf('_') + 1),
            DefinitionFingerprint = new string('1', 64),
            SourceRows = 2,
            SourceContentHash = new string('2', 64),
            CopiedRows = 0,
            CopiedContentHash = new string('3', 64),
            Completed = false
        };
        await destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: false);

        using var page = new DataTable();
        page.Columns.Add("id", typeof(long));
        page.Columns.Add("payload", typeof(string));
        page.Rows.Add(1L, "first");
        page.Rows.Add(1L, "duplicate");

        await Assert.ThrowsAnyAsync<Exception>(() => destination.CommitPageAsync(
            definition,
            page,
            new DbaTableCopyOptions { CheckpointId = checkpoint.CopyId, BatchSize = 1 },
            checkpoint,
            checkpoint with { CopiedRows = 2 }));

        Assert.Equal(0L, Convert.ToInt64(await ExecuteScalarAsync(connection, CountSql(provider, table))));
        Assert.Equal(checkpoint, await destination.ReadCheckpointAsync(definition));
    }

    private static DbaTableCopyCheckpoint CreateInitialCheckpoint(string copyId)
        => new()
        {
            CopyId = copyId,
            DefinitionFingerprint = new string('1', 64),
            SourceRows = 0,
            SourceContentHash = new string('2', 64),
            CopiedRows = 0,
            CopiedContentHash = new string('3', 64),
            Completed = false
        };

    private static DbaProviderTableCopyAdapterBase CreateAdapter(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? orderBy = null,
        DbaTableCopyReadConsistency readConsistency = DbaTableCopyReadConsistency.CallerManaged)
    {
        var options = new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = connectionString,
            DefaultOrderByColumns = orderBy,
            ReadConsistency = readConsistency
        };
        return provider switch
        {
            DbaTableCopyProvider.PostgreSql => new PostgreSqlTableCopyAdapter(options),
            DbaTableCopyProvider.MySql => new MySqlTableCopyAdapter(options),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };
    }

    private static DbConnection CreateConnection(DbaTableCopyProvider provider, string connectionString)
        => provider switch
        {
            DbaTableCopyProvider.PostgreSql => new NpgsqlConnection(connectionString),
            DbaTableCopyProvider.MySql => new MySqlConnection(connectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };

    private static string CreateTableSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"CREATE TABLE \"{table}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)"
            : $"CREATE TABLE `{table}` (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)";

    private static string DropTableSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"DROP TABLE IF EXISTS \"{table}\""
            : $"DROP TABLE IF EXISTS `{table}`";

    private static string CountSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"SELECT COUNT(*) FROM \"{table}\""
            : $"SELECT COUNT(*) FROM `{table}`";

    private static string DeleteCheckpointSql(DbaTableCopyProvider provider, string suffix)
    {
        var table = provider == DbaTableCopyProvider.PostgreSql
            ? "\"DbaClientX_TableCopyCheckpoints\""
            : "`DbaClientX_TableCopyCheckpoints`";
        return $"DELETE FROM {table} WHERE CopyId LIKE '%{suffix.Replace("'", "''")}'";
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task TryExecuteAsync(DbConnection connection, string sql)
    {
        try
        {
            await ExecuteAsync(connection, sql);
        }
        catch (DbException)
        {
            // Cleanup is best-effort so it cannot hide the provider assertion that failed.
        }
    }

    private static async Task<object?> ExecuteScalarAsync(DbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}
