using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerBulkInsertTests
{
    private class CaptureBulkCopySqlServer : DBAClientX.SqlServer
    {
        public int? BatchSize { get; private set; }
        public int? Timeout { get; private set; }
        public string? Destination { get; private set; }
        public string? ConnectionString { get; private set; }
        public SqlBulkCopyOptions Options { get; private set; }
        public int NotifyAfter { get; private set; }
        public List<(string Source, string Destination)> Mappings { get; } = new();
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override SqlConnection CreateConnection(string connectionString)
        {
            ConnectionString = connectionString;
            return new();
        }

        protected override void OpenConnection(SqlConnection connection)
        {
            // no-op to avoid real connection
        }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override SqlBulkCopy CreateBulkCopy(SqlConnection connection, SqlTransaction? transaction, SqlBulkCopyOptions options)
        {
            Options = options;
            return base.CreateBulkCopy(connection, transaction, options);
        }

        protected override void WriteToServer(SqlBulkCopy bulkCopy, DataTable table)
        {
            BatchSize = bulkCopy.BatchSize;
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            NotifyAfter = bulkCopy.NotifyAfter;
            foreach (SqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceColumn, mapping.DestinationColumn));
            }
        }

        protected override Task WriteToServerAsync(SqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, table);
            return Task.CompletedTask;
        }

        protected override void DisposeConnection(SqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private class OpenFailureBulkCopySqlServer : DBAClientX.SqlServer
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override void OpenConnection(SqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(SqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private class AutoCreateBulkCopySqlServer : CaptureBulkCopySqlServer
    {
        public List<(string CommandText, Dictionary<string, object?> Parameters)> SetupCommands { get; } = new();

        protected override void ExecuteBulkInsertSetupCommand(
            SqlConnection connection,
            SqlTransaction? transaction,
            string commandText,
            IReadOnlyDictionary<string, object?> parameters)
            => SetupCommands.Add((commandText, new Dictionary<string, object?>(parameters)));

        protected override Task ExecuteBulkInsertSetupCommandAsync(
            SqlConnection connection,
            SqlTransaction? transaction,
            string commandText,
            IReadOnlyDictionary<string, object?> parameters,
            CancellationToken cancellationToken)
        {
            SetupCommands.Add((commandText, new Dictionary<string, object?>(parameters)));
            return Task.CompletedTask;
        }
    }

    private class LegacyFactorySqlServer : DBAClientX.SqlServer
    {
        public int LegacyFactoryCalls { get; private set; }
        public int OptionsFactoryCalls { get; private set; }

        protected override SqlConnection CreateConnection(string connectionString) => new();

        protected override void OpenConnection(SqlConnection connection)
        {
            // no-op to avoid real connection
        }

        protected override SqlBulkCopy CreateBulkCopy(SqlConnection connection, SqlTransaction? transaction)
        {
            LegacyFactoryCalls++;
            return base.CreateBulkCopy(connection, transaction);
        }

        protected override SqlBulkCopy CreateBulkCopy(SqlConnection connection, SqlTransaction? transaction, SqlBulkCopyOptions options)
        {
            OptionsFactoryCalls++;
            return base.CreateBulkCopy(connection, transaction, options);
        }

        protected override void WriteToServer(SqlBulkCopy bulkCopy, DataTable table)
        {
            // no-op to avoid real connection
        }
    }

    [Fact]
    public void BulkInsert_WithAutoCreateTable_CreatesSchemaAndMappedTable()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        var displayName = table.Columns.Add("DisplayName", typeof(string));
        displayName.MaxLength = 100;
        displayName.AllowDBNull = false;
        table.Columns.Add("IsActive", typeof(bool));
        table.Columns.Add("Duration", typeof(TimeSpan));
        table.Columns.Add("Payload", typeof(byte[]));
        table.Rows.Add("Alpha", true, TimeSpan.FromSeconds(5), new byte[] { 1, 2, 3 });
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true,
            ColumnMappings = new Dictionary<string, string>
            {
                ["DisplayName"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, table, "[stage.v1].[Import Rows]", options);

        Assert.Equal(2, sqlServer.SetupCommands.Count);
        Assert.Contains("CREATE SCHEMA", sqlServer.SetupCommands[0].CommandText);
        Assert.Equal("stage.v1", sqlServer.SetupCommands[0].Parameters["@schemaName"]);
        var createTable = sqlServer.SetupCommands[1].CommandText;
        Assert.Equal("[stage.v1].[Import Rows]", sqlServer.SetupCommands[1].Parameters["@objectName"]);
        Assert.Contains("CREATE TABLE [stage.v1].[Import Rows]", createTable);
        Assert.Contains("[Name] nvarchar(100) NOT NULL", createTable);
        Assert.Contains("[IsActive] bit NULL", createTable);
        Assert.Contains("[Duration] time NULL", createTable);
        Assert.Contains("[Payload] varbinary(max) NULL", createTable);
        Assert.Contains(sqlServer.Mappings, mapping => mapping.Source == "DisplayName" && mapping.Destination == "Name");
    }

    [Fact]
    public async Task BulkInsertAsync_WithAutoCreateTable_CreatesDefaultSchemaTable()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("CreatedUtc", typeof(DateTime));
        table.Columns.Add("CorrelationId", typeof(Guid));
        table.Rows.Add(1, DateTime.UtcNow, Guid.NewGuid());
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true
        };

        await sqlServer.BulkInsertAsync("s", "db", true, table, "ImportRows", options);

        Assert.Single(sqlServer.SetupCommands);
        var createTable = sqlServer.SetupCommands[0].CommandText;
        Assert.Equal("[dbo].[ImportRows]", sqlServer.SetupCommands[0].Parameters["@objectName"]);
        Assert.Contains("CREATE TABLE [dbo].[ImportRows]", createTable);
        Assert.Contains("[Id] int NULL", createTable);
        Assert.Contains("[CreatedUtc] datetime2 NULL", createTable);
        Assert.Contains("[CorrelationId] uniqueidentifier NULL", createTable);
        Assert.Equal("[dbo].[ImportRows]", sqlServer.Destination);
    }

    [Fact]
    public void BulkInsert_WithAutoCreateTable_PreservesDecimalScale()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(decimal));
        table.Rows.Add(decimal.Parse("1.1234567890123456789012345678", CultureInfo.InvariantCulture));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.ImportRows", options);

        var createTable = sqlServer.SetupCommands[0].CommandText;
        Assert.Contains("[Amount] decimal(38,28) NULL", createTable);
    }

    [Fact]
    public void BulkInsert_WithAutoCreateTable_UsesSafeDecimalScaleWithoutSampleValues()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(decimal));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.ImportRows", options);

        var createTable = sqlServer.SetupCommands[0].CommandText;
        Assert.Contains("[Amount] decimal(38,28) NULL", createTable);
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", batchSize: 100, bulkCopyTimeout: 60);

        Assert.Equal(100, sqlServer.BatchSize);
        Assert.Equal(60, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(1, sqlServer.SyncDisposeCalls);
        Assert.Equal(0, sqlServer.AsyncDisposeCalls);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Name" && m.Destination == "Name");
    }

    [Fact]
    public async Task BulkInsertAsync_SetsOptionsAndMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        await sqlServer.BulkInsertAsync("s", "db", true, table, "dbo.Dest", batchSize: 50, bulkCopyTimeout: 30);

        Assert.Equal(50, sqlServer.BatchSize);
        Assert.Equal(30, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(0, sqlServer.SyncDisposeCalls);
        Assert.Equal(1, sqlServer.AsyncDisposeCalls);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Name" && m.Destination == "Name");
    }

    [Fact]
    public void BulkInsert_DefaultOptions_AddsAllMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest");

        Assert.Equal(0, sqlServer.BatchSize);
        Assert.Equal(30, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(table.Columns.Count, sqlServer.Mappings.Count);
    }

    [Fact]
    public void BulkInsert_WithoutOptions_UsesLegacyBulkCopyFactory()
    {
        using var sqlServer = new LegacyFactorySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);

        sqlServer.BulkInsert("Server=s;Database=db;Encrypt=True", table, "dbo.Dest");

        Assert.Equal(1, sqlServer.LegacyFactoryCalls);
        Assert.Equal(0, sqlServer.OptionsFactoryCalls);
    }

    [Fact]
    public void BulkInsert_WithBulkCopyOptions_UsesOptionsBulkCopyFactory()
    {
        using var sqlServer = new LegacyFactorySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            BulkCopyOptions = SqlBulkCopyOptions.TableLock
        };

        sqlServer.BulkInsert("Server=s;Database=db;Encrypt=True", table, "dbo.Dest", options);

        Assert.Equal(0, sqlServer.LegacyFactoryCalls);
        Assert.Equal(1, sqlServer.OptionsFactoryCalls);
    }

    [Fact]
    public void BulkInsert_WithOptions_AppliesBulkCopyOptionsMappingsAndNotifyAfter()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "test");
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            BulkCopyOptions = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity,
            NotifyAfter = 25,
            ColumnMappings = new Dictionary<string, string>
            {
                ["DisplayName"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options);

        Assert.Equal(SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity, sqlServer.Options);
        Assert.Equal(25, sqlServer.NotifyAfter);
        Assert.Equal(2, sqlServer.Mappings.Count);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "DisplayName" && m.Destination == "Name");
    }

    [Fact]
    public void BulkInsert_WithOptions_AppliesColumnMappingsCaseSensitively()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Columns.Add("displayname", typeof(string));
        table.Rows.Add(1, "Upper", "Lower");
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["displayname"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options);

        Assert.Equal(3, sqlServer.Mappings.Count);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "DisplayName" && m.Destination == "DisplayName");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "displayname" && m.Destination == "Name");
    }

    [Fact]
    public async Task BulkInsertAsync_WithOptions_AppliesBulkCopyOptionsMappingsAndNotifyAfter()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "test");
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            BulkCopyOptions = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers,
            NotifyAfter = 10,
            ColumnMappings = new Dictionary<string, string>
            {
                ["DisplayName"] = "Name"
            }
        };

        await sqlServer.BulkInsertAsync("s", "db", true, table, "dbo.Dest", options);

        Assert.Equal(SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers, sqlServer.Options);
        Assert.Equal(10, sqlServer.NotifyAfter);
        Assert.Equal(2, sqlServer.Mappings.Count);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "DisplayName" && m.Destination == "Name");
    }

    [Fact]
    public void BulkInsert_WithInvalidColumnMappingSource_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["Missing"] = "Id"
            }
        };

        Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options));
    }

    [Fact]
    public void BulkInsert_WithColumnMappingCollisionWithPassthroughColumn_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        using var table = new DataTable();
        table.Columns.Add("A", typeof(int));
        table.Columns.Add("B", typeof(int));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["A"] = "B"
            }
        };

        var exception = Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options));
        Assert.Contains("duplicate destination column 'B'", exception.Message);
    }

    [Fact]
    public void BulkInsert_WithDuplicateMappedDestinationColumns_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        using var table = new DataTable();
        table.Columns.Add("A", typeof(int));
        table.Columns.Add("B", typeof(int));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["A"] = "C",
                ["B"] = "C"
            }
        };

        var exception = Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options));
        Assert.Contains("duplicate destination column 'C'", exception.Message);
    }

    [Fact]
    public void BulkInsert_WithConnectionString_UsesConnectionStringOverload()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);

        sqlServer.BulkInsert("Server=.;Database=DDCT;Integrated Security=True;Application Name=DDCT;", table, "dbo.Dest");

        Assert.Equal("Server=.;Database=DDCT;Integrated Security=True;Application Name=DDCT;", sqlServer.ConnectionString);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(1, sqlServer.SyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WithConnectionString_UsesConnectionStringOverload()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);

        await sqlServer.BulkInsertAsync("Server=.;Database=DDCT;Integrated Security=True;Application Name=DDCT;", table, "dbo.Dest");

        Assert.Equal("Server=.;Database=DDCT;Integrated Security=True;Application Name=DDCT;", sqlServer.ConnectionString);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(1, sqlServer.AsyncDisposeCalls);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", useTransaction: true));
    }

    [Fact]
    public void BulkInsert_WhenOpenFails_DisposesConnection()
    {
        using var sqlServer = new OpenFailureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, sqlServer.SyncDisposeCalls);
        Assert.Equal(0, sqlServer.AsyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WhenOpenFails_DisposesConnection()
    {
        using var sqlServer = new OpenFailureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => sqlServer.BulkInsertAsync("s", "db", true, table, "dbo.Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, sqlServer.SyncDisposeCalls);
        Assert.Equal(1, sqlServer.AsyncDisposeCalls);
    }

    [Fact]
    public void BulkInsert_WithEmptyDestination_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, " "));
    }

    [Fact]
    public void BulkInsert_WithNoColumns_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var table = new DataTable();

        Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest"));
    }
}
