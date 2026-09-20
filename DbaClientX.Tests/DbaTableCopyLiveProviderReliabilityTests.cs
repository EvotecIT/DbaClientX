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

            var providerSource = CreateAdapter(
                provider,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            using (await ((IDbaTableCopyReadSession)providerSource).OpenReadSessionAsync())
            {
                var providerDefinition = new DbaTableCopyDefinition(
                    destinationTable,
                    destinationTable,
                    new[] { "id" })
                {
                    UseKeysetPagination = true
                };
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
