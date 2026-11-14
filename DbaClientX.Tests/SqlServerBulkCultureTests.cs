using System.Data;
using System.Globalization;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

// Values crossing between text and typed SQL Server columns must not depend on the session culture: under pl-PL
// SqlBulkCopy wrote 0.25 as "0,25" and could not load "1.25" into a decimal.
public sealed class SqlServerBulkCultureTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_TypedSourceIntoTextColumnsUnderPolishCulture_WritesWhatCastWouldProduce()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxCultureSource" + suffix;
        string destination = "dbo.DbaxCultureText" + suffix;
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL PRIMARY KEY, Amount decimal(18,4) NULL, Ratio float NULL, Created datetime2 NULL,
    Seen datetimeoffset NULL, At time NULL, Flag bit NULL, Code uniqueidentifier NULL, Delta int NULL);
CREATE TABLE {destination} (Id int NOT NULL PRIMARY KEY, Amount nvarchar(60) NULL, Ratio nvarchar(60) NULL, Created nvarchar(60) NULL,
    Seen nvarchar(60) NULL, At nvarchar(60) NULL, Flag nvarchar(60) NULL, Code nvarchar(60) NULL, Delta nvarchar(60) NULL);
INSERT INTO {source} VALUES
    (1, 1234.2500, 0.1, '2026-11-14T13:05:06.7891234', '2026-11-14T13:05:06.5+01:00', '13:05:06.789', 1, 'A0B1C2D3-E4F5-4678-9ABC-DEF012345678', -1234567),
    (2, -0.0001, 1234567.89, '2026-01-02T03:04:05', '2026-01-02T03:04:05-05:30', '00:00:00', 0, NEWID(), 0),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);");

            DbaTableCopyResult result = await UnderPolishCulture(() => new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }) },
                new DbaTableCopyOptions()));

            Assert.Equal(3, result.CopiedRows);
            // Every column equals SQL Server's own CAST to nvarchar, except float, which keeps all its digits.
            object? mismatches = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM {source} AS s JOIN {destination} AS d ON d.Id = s.Id
WHERE ISNULL(d.Amount, N'~') <> ISNULL(CAST(s.Amount AS nvarchar(60)), N'~')
   OR ISNULL(CAST(d.Ratio AS float), -1) <> ISNULL(s.Ratio, -1)
   OR ISNULL(d.Created, N'~') <> ISNULL(CAST(s.Created AS nvarchar(60)), N'~')
   OR ISNULL(d.Seen, N'~') <> ISNULL(CAST(s.Seen AS nvarchar(60)), N'~')
   OR ISNULL(d.At, N'~') <> ISNULL(CAST(s.At AS nvarchar(60)), N'~')
   OR ISNULL(d.Flag, N'~') <> ISNULL(CAST(s.Flag AS nvarchar(60)), N'~')
   OR ISNULL(d.Code, N'~') <> ISNULL(CAST(s.Code AS nvarchar(60)), N'~')
   OR ISNULL(d.Delta, N'~') <> ISNULL(CAST(s.Delta AS nvarchar(60)), N'~')");
            Assert.Equal(0, Convert.ToInt32(mismatches, CultureInfo.InvariantCulture));
            Assert.Equal("1234567.89", await sql.ExecuteScalarAsync(connectionString!, $"SELECT Ratio FROM {destination} WHERE Id = 2"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_TextIntoTypedColumnsUnderPolishCulture_ParsesInvariantText(bool asReader)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxCultureTyped" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        foreach (string name in new[] { "Id", "Amount", "Ratio", "Created", "Seen" }) rows.Columns.Add(name, typeof(string));
        rows.Rows.Add("1", "1234.25", "0.1", "2026-11-14 13:05:06.789", "2026-11-14 13:05:06 +01:00");
        // Text in the session's own format that is not valid invariant text still loads as before.
        rows.Rows.Add("2", "14,5", "2,5", "14.11.2026 13:05:06", "2026-11-14 13:05:06 +01:00");
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!,
                $"CREATE TABLE {destination} (Id int NOT NULL, Amount decimal(18,4) NULL, Ratio float NULL, Created datetime2 NULL, Seen datetimeoffset NULL)");

            await UnderPolishCulture(async () =>
            {
                if (asReader)
                {
                    using IDataReader reader = rows.CreateDataReader();
                    await sql.BulkInsertAsync(connectionString!, reader, destination, options: null);
                }
                else
                {
                    await sql.BulkInsertAsync(connectionString!, rows, destination, options: null);
                }

                return true;
            });

            object? matching = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM {destination}
WHERE (Id = 1 AND Amount = 1234.25 AND Ratio = 0.1 AND Created = '2026-11-14T13:05:06.789' AND Seen = '2026-11-14T13:05:06+01:00')
   OR (Id = 2 AND Amount = 14.5 AND Ratio = 2.5 AND Created = '2026-11-14T13:05:06')");
            Assert.Equal(2, Convert.ToInt32(matching, CultureInfo.InvariantCulture));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}");
        }
    }

    private static async Task<T> UnderPolishCulture<T>(Func<Task<T>> action)
    {
        CultureInfo culture = CultureInfo.CurrentCulture, uiCulture = CultureInfo.CurrentUICulture;
        CultureInfo.CurrentCulture = CultureInfo.CurrentUICulture = new CultureInfo("pl-PL");
        try
        {
            return await action();
        }
        finally
        {
            CultureInfo.CurrentCulture = culture;
            CultureInfo.CurrentUICulture = uiCulture;
        }
    }
}
