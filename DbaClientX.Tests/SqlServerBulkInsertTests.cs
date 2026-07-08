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
        public int ReaderRows { get; private set; }
        public int ReaderFieldCount { get; private set; }
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

        protected override void WriteToServer(SqlBulkCopy bulkCopy, IDataReader reader)
        {
            BatchSize = bulkCopy.BatchSize;
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            NotifyAfter = bulkCopy.NotifyAfter;
            ReaderFieldCount = reader.FieldCount;
            foreach (SqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceOrdinal.ToString(CultureInfo.InvariantCulture), mapping.DestinationColumn));
            }

            while (reader.Read())
            {
                ReaderRows++;
            }
        }

        protected override Task WriteToServerAsync(SqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, table);
            return Task.CompletedTask;
        }

        protected override Task WriteToServerAsync(SqlBulkCopy bulkCopy, IDataReader reader, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, reader);
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

    private sealed class UnnamedColumnReader : IDataReader
    {
        private readonly string[] _names;
        private bool _read;

        public UnnamedColumnReader(bool blankName)
            : this(blankName ? new[] { string.Empty } : new[] { "Id" })
        {
        }

        public UnnamedColumnReader(string[] names)
            => _names = names;

        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public int Depth => 0;

        public bool IsClosed { get; private set; }

        public int RecordsAffected => -1;

        public int FieldCount => _names.Length;

        public void Close() => IsClosed = true;

        public void Dispose() => Close();

        public bool GetBoolean(int i) => (int)GetValue(i) != 0;

        public byte GetByte(int i) => (byte)GetValue(i);

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();

        public char GetChar(int i) => (char)GetValue(i);

        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();

        public IDataReader GetData(int i) => throw new NotSupportedException();

        public string GetDataTypeName(int i) => GetFieldType(i).Name;

        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);

        public decimal GetDecimal(int i) => (decimal)GetValue(i);

        public double GetDouble(int i) => (double)GetValue(i);

        public Type GetFieldType(int i) => typeof(int);

        public float GetFloat(int i) => (float)GetValue(i);

        public Guid GetGuid(int i) => (Guid)GetValue(i);

        public short GetInt16(int i) => (short)GetValue(i);

        public int GetInt32(int i) => (int)GetValue(i);

        public long GetInt64(int i) => (long)GetValue(i);

        public string GetName(int i) => _names[i];

        public int GetOrdinal(string name) => string.Equals(name, GetName(0), StringComparison.OrdinalIgnoreCase) ? 0 : -1;

        public DataTable GetSchemaTable()
        {
            var schema = new DataTable();
            schema.Columns.Add("ColumnName", typeof(string));
            schema.Columns.Add("ColumnOrdinal", typeof(int));
            schema.Columns.Add("DataType", typeof(Type));
            schema.Columns.Add("AllowDBNull", typeof(bool));
            schema.Columns.Add("ColumnSize", typeof(int));
            for (var index = 0; index < _names.Length; index++)
            {
                schema.Rows.Add(GetName(index), index, typeof(int), false, 0);
            }

            return schema;
        }

        public string GetString(int i) => GetValue(i).ToString()!;

        public object GetValue(int i) => 42 + i;

        public int GetValues(object[] values)
        {
            var count = Math.Min(values.Length, _names.Length);
            for (var index = 0; index < count; index++)
            {
                values[index] = GetValue(index);
            }

            return count;
        }

        public bool IsDBNull(int i) => false;

        public bool NextResult() => false;

        public bool Read()
        {
            if (_read)
            {
                return false;
            }

            _read = true;
            return true;
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
    public void BulkInsert_WithDataReaderAutoCreate_CreatesSchemaFromReader()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        var displayName = table.Columns.Add("DisplayName", typeof(string));
        displayName.MaxLength = 100;
        displayName.AllowDBNull = false;
        table.Columns.Add("IsActive", typeof(bool));
        table.Rows.Add("Alpha", true);
        using var reader = table.CreateDataReader();
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true,
            ColumnMappings = new Dictionary<string, string>
            {
                ["DisplayName"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, reader, "[stage].[ImportRows]", options);

        Assert.Equal(2, sqlServer.SetupCommands.Count);
        var createTable = sqlServer.SetupCommands[1].CommandText;
        Assert.Equal("[stage].[ImportRows]", sqlServer.SetupCommands[1].Parameters["@objectName"]);
        Assert.Contains("CREATE TABLE [stage].[ImportRows]", createTable);
        Assert.Contains("[Name] nvarchar(100) NOT NULL", createTable);
        Assert.Contains("[IsActive] bit NULL", createTable);
        Assert.Equal(1, sqlServer.ReaderRows);
        Assert.Contains(sqlServer.Mappings, mapping => mapping.Source == "0" && mapping.Destination == "Name");
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
        Assert.Contains("[Amount] decimal(38,18) NULL", createTable);
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
        Assert.Contains("[Amount] decimal(38,18) NULL", createTable);
    }

    [Fact]
    public void BulkInsert_WithAutoCreateTable_UsesSafeDecimalScaleForWholeNumberSamples()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(decimal));
        table.Rows.Add(1m);
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.ImportRows", options);

        var createTable = sqlServer.SetupCommands[0].CommandText;
        Assert.Contains("[Amount] decimal(38,18) NULL", createTable);
    }

    [Fact]
    public void BulkInsert_WithAutoCreateTable_UsesDecimalScaleThatSupportsLargeIntegerParts()
    {
        using var sqlServer = new AutoCreateBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(decimal));
        table.Rows.Add(decimal.Parse("1234567890123.1234567890123456", CultureInfo.InvariantCulture));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            AutoCreateTable = true
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.ImportRows", options);

        var createTable = sqlServer.SetupCommands[0].CommandText;
        Assert.Contains("[Amount] decimal(38,18) NULL", createTable);
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
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
    public void BulkInsert_WithDataReader_StreamsRowsAndAppliesMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "Alpha");
        table.Rows.Add(2, "Beta");
        using var reader = table.CreateDataReader();
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            BulkCopyOptions = SqlBulkCopyOptions.TableLock,
            NotifyAfter = 25,
            ColumnMappings = new Dictionary<string, string>
            {
                ["DisplayName"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, reader, "dbo.Dest", options, batchSize: 100, bulkCopyTimeout: 60);

        Assert.Equal(SqlBulkCopyOptions.TableLock, sqlServer.Options);
        Assert.Equal(100, sqlServer.BatchSize);
        Assert.Equal(60, sqlServer.Timeout);
        Assert.Equal(25, sqlServer.NotifyAfter);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(2, sqlServer.ReaderFieldCount);
        Assert.Equal(2, sqlServer.ReaderRows);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "0" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "1" && m.Destination == "Name");
    }

    [Fact]
    public async Task BulkInsertAsync_WithDataReader_StreamsRowsAndDisposesConnection()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        table.Rows.Add(2);
        using var reader = table.CreateDataReader();

        await sqlServer.BulkInsertAsync("Server=s;Database=db;Encrypt=True", reader, "dbo.Dest");

        Assert.Equal("Server=s;Database=db;Encrypt=True", sqlServer.ConnectionString);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(2, sqlServer.ReaderRows);
        Assert.Equal(0, sqlServer.SyncDisposeCalls);
        Assert.Equal(1, sqlServer.AsyncDisposeCalls);
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
    public void BulkInsert_WithOptions_AppliesColumnMappingsWithConfiguredComparer()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "Upper");
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["displayname"] = "Name"
            }
        };

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options);

        Assert.Equal(2, sqlServer.Mappings.Count);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "DisplayName" && m.Destination == "Name");
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
    public void BulkInsert_WithUnnamedReaderColumn_AllowsMappingSynthesizedSourceName()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var reader = new UnnamedColumnReader(blankName: true);
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["Column1"] = "Id"
            }
        };

        sqlServer.BulkInsert("s", "db", true, reader, "dbo.Dest", options);

        Assert.Contains(sqlServer.Mappings, mapping => mapping.Source == "0" && mapping.Destination == "Id");
    }

    [Fact]
    public void BulkInsert_WithUnnamedReaderColumn_KeepsSynthesizedSourceNamesUnique()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        using var reader = new UnnamedColumnReader(new[] { string.Empty, "Column1" });
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["Column1"] = "GeneratedId",
                ["Column1_2"] = "ExistingColumn"
            }
        };

        sqlServer.BulkInsert("s", "db", true, reader, "dbo.Dest", options);

        Assert.Contains(sqlServer.Mappings, mapping => mapping.Source == "0" && mapping.Destination == "GeneratedId");
        Assert.Contains(sqlServer.Mappings, mapping => mapping.Source == "1" && mapping.Destination == "ExistingColumn");
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
    public void BulkInsert_WithCaseOnlyDuplicateMappedDestinationColumns_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        using var table = new DataTable();
        table.Columns.Add("A", typeof(int));
        table.Columns.Add("B", typeof(int));
        var options = new DBAClientX.SqlServerBulkInsertOptions
        {
            ColumnMappings = new Dictionary<string, string>
            {
                ["A"] = "Name",
                ["B"] = "name"
            }
        };

        var exception = Assert.Throws<ArgumentException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", options));
        Assert.Contains("duplicate destination column 'name'", exception.Message);
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
