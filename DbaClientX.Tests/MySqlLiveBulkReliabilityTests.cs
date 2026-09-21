using System.Data;
using DBAClientX;
using MySqlConnector;

namespace DbaClientX.Tests;

public sealed class MySqlLiveBulkReliabilityTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task StandaloneBulkInsert_RollsBackConversionWarnings()
    {
        string? configuredConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(configuredConnectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var builder = new MySqlConnectionStringBuilder(configuredConnectionString!)
        {
            AllowLoadLocalInfile = true
        };
        string tableName = "dbax_warning_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{tableName}` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(3) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{tableName}` VALUES (99, 'old')");
            using var page = new DataTable();
            page.Columns.Add("id", typeof(long));
            page.Columns.Add("payload", typeof(string));
            page.Rows.Add(1L, "too-long");
            using var mySql = new MySql();

            var exception = await Assert.ThrowsAsync<DbaQueryExecutionException>(() =>
                mySql.BulkInsertAsync(builder.ConnectionString, page, tableName));

            Assert.Contains("bulk insert", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{tableName}`")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT id FROM `{tableName}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{tableName}`");
        }
    }

    private static async Task ExecuteAsync(MySqlConnection connection, string sql)
    {
        await using var command = new MySqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ExecuteScalarAsync(MySqlConnection connection, string sql)
    {
        await using var command = new MySqlCommand(sql, connection);
        return await command.ExecuteScalarAsync();
    }

    private static async Task TryExecuteAsync(MySqlConnection connection, string sql)
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
