using System.Data;
using DBAClientX;

namespace DbaClientX.Tests;

// Auto-created text columns are sized from the data and widened, never truncated, when later rows need more room.
public sealed class SqlServerBulkAutoSizeTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_AutoCreatedTextColumns_SizeFromDataAndWidenForLaterWrites()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxAutoSize" + Guid.NewGuid().ToString("N");
        var options = new SqlServerBulkInsertOptions { AutoCreateTable = true };
        using var sql = new SqlServer();
        try
        {
            using (DataTable first = CreateRows(("Zażółć", null), (null, "short")))
            {
                await sql.BulkInsertAsync(connectionString!, first, destination, options);
            }

            Assert.Equal(new[] { "16", "16" }, await GetTextLengthsAsync(sql, connectionString!, destination));

            // A write without values for a column leaves that column as it is.
            using (DataTable nullNotes = CreateRows(("abc", null)))
            {
                await sql.BulkInsertAsync(connectionString!, nullNotes, destination, options);
            }

            Assert.Equal(new[] { "16", "16" }, await GetTextLengthsAsync(sql, connectionString!, destination));

            string medium = new('m', 300);
            string large = new('l', 5000);
            using (DataTable second = CreateRows((medium, large)))
            {
                await sql.BulkInsertAsync(connectionString!, second, destination, options);
            }

            Assert.Equal(new[] { "512", "max" }, await GetTextLengthsAsync(sql, connectionString!, destination));
            object? stored = await sql.ExecuteScalarAsync(connectionString!,
                $"SELECT COUNT(*) FROM {destination} WHERE (Name = N'Zażółć' AND Notes IS NULL) OR (Name IS NULL AND Notes = N'short') OR (Name = N'abc' AND Notes IS NULL) OR (Name = REPLICATE(N'm', 300) AND Notes = REPLICATE(CAST(N'l' AS nvarchar(max)), 5000))");
            Assert.Equal(4, Convert.ToInt32(stored));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertWithResultAsync_ReaderTextLongerThanSample_WidensAndCopiesEveryRow()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxAutoSizeReader" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        rows.Columns.Add("Id", typeof(int));
        rows.Columns.Add("Name", typeof(string));
        for (int id = 1; id <= 1500; id++)
        {
            rows.Rows.Add(id, id switch { 1200 => new string('x', 100), 1400 => new string('y', 5000), _ => "Row " + id });
        }

        var progress = new List<long>();
        var options = new SqlServerBulkInsertOptions { AutoCreateTable = true, NotifyAfter = 100, RowsCopied = progress.Add };
        using var sql = new SqlServer();
        try
        {
            using IDataReader reader = rows.CreateDataReader();
            SqlServerBulkInsertResult result = await sql.BulkInsertWithResultAsync(connectionString!, reader, destination, options);

            Assert.Equal(1500, result.RowsCopied);
            Assert.Equal(new[] { "max" }, await GetTextLengthsAsync(sql, connectionString!, destination));
            Assert.Equal(progress.OrderBy(static value => value), progress);
            // Progress continues across the widening restarts: 1,399 rows precede the last segment's first notification.
            Assert.Contains(1499, progress);
            object? matching = await sql.ExecuteScalarAsync(connectionString!,
                $"SELECT COUNT(*) FROM {destination} WHERE Name = CASE Id WHEN 1200 THEN REPLICATE(N'x', 100) WHEN 1400 THEN REPLICATE(CAST(N'y' AS nvarchar(max)), 5000) ELSE CONCAT(N'Row ', Id) END");
            Assert.Equal(1500, Convert.ToInt32(matching));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}");
        }
    }

    private static DataTable CreateRows(params (string? Name, string? Notes)[] values)
    {
        var table = new DataTable();
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Notes", typeof(string));
        foreach ((string? name, string? notes) in values)
        {
            table.Rows.Add((object?)name ?? DBNull.Value, (object?)notes ?? DBNull.Value);
        }

        return table;
    }

    private static async Task<string[]> GetTextLengthsAsync(SqlServer _, string connectionString, string table)
    {
        using var sql = new SqlServer { ReturnType = ReturnType.DataTable };
        var result = (DataTable)(await sql.QueryAsync(connectionString,
            $"SELECT CASE WHEN max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(10), max_length / 2) END AS length FROM sys.columns WHERE object_id = OBJECT_ID(N'{table}') AND system_type_id = 231 ORDER BY column_id"))!;
        return result.Rows.Cast<DataRow>().Select(static row => (string)row[0]).ToArray();
    }
}
