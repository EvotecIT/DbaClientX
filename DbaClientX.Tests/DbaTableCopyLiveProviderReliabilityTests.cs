using System.Data;
using System.Data.Common;
using DBAClientX;
using DBAClientX.DataMovement;
using MySqlConnector;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyLiveProviderReliabilityTests
{
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
