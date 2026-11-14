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

    // A native sequential reader must be read in destination order, and SqlBulkCopy rejects it when a character
    // destination's collation locale differs from the source column, including a non-character source; those copy
    // through pages instead.
    [Theory]
    [InlineData("Reordered")]
    [InlineData("DifferentLocale")]
    [InlineData("NumberToText")]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_SqlServerDestinationLayoutDiffers_CopiesEveryRow(string layout)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxStreamLayoutSource" + suffix;
        string destination = "dbo.DbaxStreamLayoutDestination" + suffix;
        string destinationCollation = layout == "DifferentLocale" ? "Polish_CI_AS" : "Latin1_General_CI_AS";
        string sourceAmount = layout == "NumberToText" ? "int" : "decimal(9,2)";
        string destinationAmount = layout == "NumberToText" ? "nvarchar(20) COLLATE Latin1_General_CI_AS" : "decimal(9,2)";
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL PRIMARY KEY, Name nvarchar(50) COLLATE Latin1_General_CI_AS NULL, Amount {sourceAmount} NULL);
CREATE TABLE {destination} (Amount {destinationAmount} NULL, Name nvarchar(50) COLLATE {destinationCollation} NULL, Id int NOT NULL PRIMARY KEY);
WITH numbers AS (SELECT TOP (2500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO {source} (Id, Name, Amount)
SELECT n, CASE WHEN n % 3 = 0 THEN NULL ELSE CONCAT(N'Row ', n) END, n / 4.0 FROM numbers;");

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }) },
                new DbaTableCopyOptions { PageSize = 1000 });

            Assert.True(result.Verified);
            Assert.Equal(2500, result.CopiedRows);
            Assert.Equal(layout == "Reordered" ? 1 : 3, Assert.Single(result.Tables).PageCount);
            object? differences = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM (
    (SELECT Id, Name COLLATE Latin1_General_BIN2 AS Name, Amount FROM {source} EXCEPT SELECT Id, Name COLLATE Latin1_General_BIN2, Amount FROM {destination})
    UNION ALL
    (SELECT Id, Name COLLATE Latin1_General_BIN2 AS Name, Amount FROM {destination} EXCEPT SELECT Id, Name COLLATE Latin1_General_BIN2, Amount FROM {source})) AS differences");
            Assert.Equal(0, Convert.ToInt32(differences));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertWithResultAsync_ReaderCanceledMidCopy_StopsAndThrowsCancellation()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxBulkCancel" + Guid.NewGuid().ToString("N");
        using var rows = new System.Data.DataTable();
        rows.Columns.Add("Id", typeof(int));
        for (int id = 1; id <= 200_000; id++) rows.Rows.Add(id);
        using var sql = new SqlServer();
        using var cancellation = new CancellationTokenSource();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"CREATE TABLE {destination} (Id int NOT NULL)");
            using System.Data.IDataReader reader = rows.CreateDataReader();
            var options = new SqlServerBulkInsertOptions { NotifyAfter = 1000, RowsCopied = _ => cancellation.Cancel() };

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sql.BulkInsertWithResultAsync(
                connectionString!, reader, destination, options, batchSize: 1000, cancellationToken: cancellation.Token));

            long copied = Convert.ToInt64(await sql.ExecuteScalarAsync(connectionString!, $"SELECT COUNT_BIG(*) FROM {destination}"));
            Assert.InRange(copied, 1, 199_999);
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}");
        }
    }
}
