using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class PostgreSqlLiveKeysetReliabilityTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task KeysetRead_ResolvesAutomaticallyDelimitedColumnSpelling()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string tableName = "dbax_auto_key_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{tableName}\" (\"event-id\" bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{tableName}\" VALUES (1, 'first'), (2, 'second')");
            var source = new PostgreSqlTableCopyAdapter(connectionString!, new[] { "event-id" });
            var definition = new DbaTableCopyDefinition(tableName, tableName, new[] { "event-id" })
            {
                UseKeysetPagination = true
            };

            using DbaTableCopyPage first = await source.ReadPageAsync(new(definition, null, 1));
            using DbaTableCopyPage second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));

            Assert.Equal(1L, first.Data.Rows[0].Field<long>("event-id"));
            Assert.Equal(2L, second.Data.Rows[0].Field<long>("event-id"));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{tableName}\"");
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task TryExecuteAsync(NpgsqlConnection connection, string sql)
    {
        try
        {
            await ExecuteAsync(connection, sql);
        }
        catch
        {
        }
    }
}
