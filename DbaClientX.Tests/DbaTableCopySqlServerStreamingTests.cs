using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed class DbaTableCopySqlServerStreamingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_SqlServerToSqlServer_StreamsPlainCopiesAndPagesMappedCopies(bool mapped)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxStreamSource" + suffix;
        string destination = "dbo.DbaxStreamDestination" + suffix;
        string nameColumn = mapped ? "Label" : "Name";
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) NULL, Payload varbinary(max) NULL);
CREATE TABLE {destination} (Id int IDENTITY(1,1) PRIMARY KEY, {nameColumn} nvarchar(50) NULL, Payload varbinary(max) NULL);
WITH numbers AS (SELECT TOP (2500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO {source} (Name, Payload)
SELECT CASE WHEN n % 3 = 0 THEN NULL ELSE CONCAT(N'Zażółć ', n) END,
       CASE WHEN n % 5 = 0 THEN CAST(REPLICATE(CAST('x' AS varchar(max)), 9000) AS varbinary(max)) END
FROM numbers;
DELETE FROM {source} WHERE Id = 7;");
            var definition = new DbaTableCopyDefinition(
                source,
                destination,
                new[] { "Id" },
                ColumnMappings: mapped ? new Dictionary<string, string> { ["Name"] = "Label" } : null);
            var progress = new List<long>();

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1000, BatchSize = 700, KeepIdentity = true, Progress = item => progress.Add(item.RowsCopied) });

            Assert.True(result.Verified);
            Assert.Equal(2499, result.CopiedRows);
            Assert.Equal(mapped ? 3 : 1, Assert.Single(result.Tables).PageCount);
            Assert.Equal(2499, progress[^1]);
            object? differences = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM (
    (SELECT Id, Name, Payload FROM {source} EXCEPT SELECT Id, {nameColumn}, Payload FROM {destination})
    UNION ALL
    (SELECT Id, {nameColumn}, Payload FROM {destination} EXCEPT SELECT Id, Name, Payload FROM {source})) AS differences");
            Assert.Equal(0, Convert.ToInt32(differences));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }
}
