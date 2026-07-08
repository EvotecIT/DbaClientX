using System.Data;
using System.Management.Automation;
using DBAClientX.DataMovement;

namespace DBAClientX.PowerShell;

/// <summary>Writes tabular pipeline input to a database table using provider-native bulk insert APIs.</summary>
/// <para>Accepts a DataTable, DataView, IDataReader, DataRow pipeline, or regular PowerShell objects and routes the resulting table to the selected DbaClientX provider.</para>
/// <example>
/// <summary>Write an Excel-imported DataTable to SQL Server.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Import-OfficeExcel .\Data.xlsx -AsDataTable | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable dbo.Import</code>
/// <para>Loads the workbook rows as a DataTable and sends them through the SQL Server bulk-copy provider.</para>
/// </example>
/// <example>
/// <summary>Create a SQL Server staging table from incoming columns.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable staging.Import -AutoCreateTable -TableLock</code>
/// <para>Creates the destination schema and table when needed, then writes the rows through SQL Server bulk copy.</para>
/// </example>
/// <example>
/// <summary>Write object pipeline data to PostgreSQL.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows | Write-DbaXTableData -Provider PostgreSql -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret;SslMode=Require' -DestinationTable public.import_data -BatchSize 5000</code>
/// <para>Converts the objects to a DataTable and writes them with the PostgreSQL COPY-backed provider.</para>
/// </example>
[Cmdlet(VerbsCommunications.Write, "DbaXTableData", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletWriteDbaXTableData : PSCmdlet
{
    internal static ScriptBlock? BulkInsertOverride { get; set; }

    private readonly List<object?> _input = new();
    private ActionPreference _errorAction;

    /// <summary>Database provider that should receive the bulk insert.</summary>
    [Parameter(Mandatory = true)]
    public DbaXBulkProvider Provider { get; set; }

    /// <summary>Provider connection string used for the bulk insert.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Destination table name. Include schema or owner when required by the provider.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Tabular input to write. Accepts DataTable, DataView, IDataReader, DataRow, hashtable, and object pipeline input.</summary>
    [Parameter(ValueFromPipeline = true, Mandatory = true)]
    [AllowNull]
    public object? InputObject { get; set; }

    /// <summary>Optional number of rows per provider bulk batch.</summary>
    [Parameter]
    public int? BatchSize { get; set; }

    /// <summary>Optional provider bulk-copy timeout in seconds. SQLite does not support this option.</summary>
    [Parameter]
    public int? BulkCopyTimeout { get; set; }

    /// <summary>SQL Server-only mapping from source column names to destination column names.</summary>
    [Parameter]
    public Hashtable? ColumnMap { get; set; }

    /// <summary>SQL Server-only option to acquire a bulk update lock for the duration of the copy.</summary>
    [Parameter]
    public SwitchParameter TableLock { get; set; }

    /// <summary>SQL Server-only option to check destination constraints during the copy.</summary>
    [Parameter]
    public SwitchParameter CheckConstraints { get; set; }

    /// <summary>SQL Server-only option to fire insert triggers during the copy.</summary>
    [Parameter]
    public SwitchParameter FireTriggers { get; set; }

    /// <summary>SQL Server-only option to preserve identity values from the source data.</summary>
    [Parameter]
    public SwitchParameter KeepIdentity { get; set; }

    /// <summary>SQL Server-only option to preserve null values from the source data.</summary>
    [Parameter]
    public SwitchParameter KeepNulls { get; set; }

    /// <summary>SQL Server-only option to create the destination schema and table when they do not already exist.</summary>
    [Parameter]
    public SwitchParameter AutoCreateTable { get; set; }

    /// <summary>SQL Server-only number of rows copied between progress updates.</summary>
    [Parameter]
    public int? NotifyAfter { get; set; }

    /// <summary>Writes a small result object with provider, destination table, and row count.</summary>
    [Parameter]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override void BeginProcessing()
    {
        _errorAction = this.ResolveErrorAction();
    }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        _input.Add(InputObject);
    }

    /// <inheritdoc />
    protected override void EndProcessing()
    {
        try
        {
            ValidateOptions();

            if (_input.Count == 0)
            {
                if (PassThru.IsPresent)
                {
                    WriteObject(new PSObject(new
                    {
                        Provider,
                        DestinationTable,
                        Rows = 0
                    }));
                }

                return;
            }

            if (!ShouldProcess(DestinationTable, $"Bulk write input using {Provider}"))
            {
                return;
            }

            if (TryWriteSqlServerReaderInput())
            {
                return;
            }

            var table = PowerShellDataTableConverter.ToDataTable(_input, DestinationTable);
            if (table.Rows.Count == 0 && !ShouldWriteSchemaOnlyTable())
            {
                if (PassThru.IsPresent)
                {
                    WriteObject(new PSObject(new
                    {
                        Provider,
                        DestinationTable,
                        Rows = 0
                    }));
                }

                return;
            }

            var providerAlias = GetProviderAlias(Provider);
            if (!TryValidateProviderConnection(providerAlias))
            {
                return;
            }

            if (Provider == DbaXBulkProvider.MySql &&
                !PowerShellHelpers.TryRequireMySqlBulkCopyLocalInfile(this, ConnectionString, _errorAction))
            {
                return;
            }

            var startedAt = DateTimeOffset.UtcNow;
            var timer = System.Diagnostics.Stopwatch.StartNew();
            var bulkTable = PrepareProviderBulkTable(table);
            using var disposableBulkTable = ReferenceEquals(bulkTable, table) ? null : bulkTable;
            if (BulkInsertOverride != null)
            {
                BulkInsertOverride.InvokeWithContext(
                    functionsToDefine: null,
                    variablesToDefine: null,
                    args: new object?[] { this, bulkTable, BuildSqlServerOptions(bulkTable) });
            }
            else
            {
                InvokeProviderBulkInsert(bulkTable);
            }

            timer.Stop();

            CompleteProgressIfNeeded();

            if (PassThru.IsPresent)
            {
                WriteObject(new PSObject(new
                {
                    Provider,
                    DestinationTable,
                    Rows = table.Rows.Count,
                    StartedAt = startedAt,
                    CompletedAt = DateTimeOffset.UtcNow,
                    ElapsedMilliseconds = Math.Round(timer.Elapsed.TotalMilliseconds, 2)
                }));
            }
        }
        catch (Exception ex)
        {
            WriteWarning($"Write-DbaXTableData - Error writing table data: {ex.Message}");
            if (_errorAction == ActionPreference.Stop)
            {
                throw;
            }
        }
    }

    private bool ShouldWriteSchemaOnlyTable()
        => Provider == DbaXBulkProvider.SqlServer && AutoCreateTable.IsPresent;

    private bool TryWriteSqlServerReaderInput()
    {
        if (Provider != DbaXBulkProvider.SqlServer || _input.Count != 1)
        {
            return false;
        }

        if (!TryGetSingleSqlServerReader(out var reader))
        {
            return false;
        }

        if (!TryValidateProviderConnection("sqlserver"))
        {
            return true;
        }

        var startedAt = DateTimeOffset.UtcNow;
        var timer = System.Diagnostics.Stopwatch.StartNew();
        var countingReader = new CountingDataReader(reader);
        var sqlServerOptions = BuildSqlServerOptions(totalRows: null);
        if (BulkInsertOverride != null)
        {
            BulkInsertOverride.InvokeWithContext(
                functionsToDefine: null,
                variablesToDefine: null,
                args: new object?[] { this, countingReader, sqlServerOptions });
        }
        else
        {
            InvokeSqlServerReaderBulkInsert(countingReader, sqlServerOptions);
        }

        timer.Stop();

        CompleteProgressIfNeeded();

        if (PassThru.IsPresent)
        {
            WriteObject(new PSObject(new
            {
                Provider,
                DestinationTable,
                Rows = countingReader.RowsRead,
                StartedAt = startedAt,
                CompletedAt = DateTimeOffset.UtcNow,
                ElapsedMilliseconds = Math.Round(timer.Elapsed.TotalMilliseconds, 2)
            }));
        }

        return true;
    }

    private bool TryGetSingleSqlServerReader(out IDataReader reader)
    {
        reader = null!;
        var candidate = PowerShellDataTableConverter.UnwrapInput(_input[0]);
        if (candidate is object?[] { Length: 1 } singleItemArray &&
            PowerShellDataTableConverter.UnwrapInput(singleItemArray[0]) is IDataReader wrappedReader)
        {
            reader = wrappedReader;
            return true;
        }

        return false;
    }

    private void ValidateOptions()
    {
        if (BatchSize.HasValue && BatchSize.Value <= 0)
        {
            throw new PSArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));
        }

        if (BulkCopyTimeout.HasValue && BulkCopyTimeout.Value <= 0)
        {
            throw new PSArgumentException("BulkCopyTimeout must be greater than zero.", nameof(BulkCopyTimeout));
        }

        if (Provider == DbaXBulkProvider.SQLite && BulkCopyTimeout.HasValue)
        {
            throw new PSArgumentException("SQLite bulk inserts do not support BulkCopyTimeout.", nameof(BulkCopyTimeout));
        }

        if (NotifyAfter.HasValue && NotifyAfter.Value <= 0)
        {
            throw new PSArgumentException("NotifyAfter must be greater than zero.", nameof(NotifyAfter));
        }

        if (Provider != DbaXBulkProvider.SqlServer && HasSqlServerOnlyOptions())
        {
            throw new PSArgumentException("ColumnMap, TableLock, CheckConstraints, FireTriggers, KeepIdentity, KeepNulls, AutoCreateTable, and NotifyAfter are only supported for the SqlServer provider.");
        }
    }

    private void InvokeProviderBulkInsert(DataTable table)
    {
        switch (Provider)
        {
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer())
                {
                    var sqlServerOptions = BuildSqlServerOptions(table);
                    if (sqlServerOptions == null)
                    {
                        sqlServer.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                    }
                    else
                    {
                        sqlServer.BulkInsert(ConnectionString, table, DestinationTable, sqlServerOptions, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                    }
                }
                break;
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql())
                {
                    postgreSql.BulkInsert(
                        ConnectionString,
                        table,
                        DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(DestinationTable),
                        batchSize: BatchSize,
                        bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql())
                {
                    mySql.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle())
                {
                    oracle.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.SQLite:
                using (var sqlite = new DBAClientX.SQLite())
                {
                    sqlite.BulkInsertWithConnectionString(ConnectionString, table, DestinationTable, batchSize: BatchSize);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{Provider}' is not supported.", nameof(Provider));
        }
    }

    private void InvokeSqlServerReaderBulkInsert(IDataReader reader, SqlServerBulkInsertOptions? sqlServerOptions)
    {
        using var sqlServer = new DBAClientX.SqlServer();
        if (sqlServerOptions == null)
        {
            sqlServer.BulkInsert(ConnectionString, reader, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
            return;
        }

        sqlServer.BulkInsert(ConnectionString, reader, DestinationTable, sqlServerOptions, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
    }

    private static string GetProviderAlias(DbaXBulkProvider provider)
        => provider switch
        {
            DbaXBulkProvider.SqlServer => "sqlserver",
            DbaXBulkProvider.PostgreSql => "postgresql",
            DbaXBulkProvider.MySql => "mysql",
            DbaXBulkProvider.Oracle => "oracle",
            DbaXBulkProvider.SQLite => "sqlite",
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    private DataTable PrepareProviderBulkTable(DataTable table)
        => Provider == DbaXBulkProvider.PostgreSql
            ? DbaPostgreSqlBulkCopyNormalizer.NormalizePage(table, DestinationTable)
            : table;

    private SqlServerBulkInsertOptions? BuildSqlServerOptions(DataTable table)
        => BuildSqlServerOptions(table.Rows.Count);

    private SqlServerBulkInsertOptions? BuildSqlServerOptions(int? totalRows)
    {
        if (Provider != DbaXBulkProvider.SqlServer || !HasSqlServerOnlyOptions())
        {
            return null;
        }

        return SqlServerBulkInsertOptionFactory.Create(
            TableLock.IsPresent,
            CheckConstraints.IsPresent,
            FireTriggers.IsPresent,
            KeepIdentity.IsPresent,
            KeepNulls.IsPresent,
            AutoCreateTable.IsPresent,
            ConvertColumnMap(),
            NotifyAfter,
            NotifyAfter.HasValue ? rowsCopied => WriteRowsCopiedProgress(totalRows, rowsCopied) : null);
    }

    private bool TryValidateProviderConnection(string providerAlias)
        => PowerShellHelpers.TryValidateConnection(
            this,
            providerAlias,
            ConnectionString,
            _errorAction,
            allowedUnsupportedOptions: Provider == DbaXBulkProvider.MySql ? PowerShellHelpers.MySqlBulkCopyAllowedUnsupportedOptions : null);

    private bool HasSqlServerOnlyOptions()
        => ColumnMap is { Count: > 0 } ||
           TableLock.IsPresent ||
           CheckConstraints.IsPresent ||
           FireTriggers.IsPresent ||
           KeepIdentity.IsPresent ||
           KeepNulls.IsPresent ||
           AutoCreateTable.IsPresent ||
           NotifyAfter.HasValue;

    private Dictionary<string, string>? ConvertColumnMap()
    {
        if (ColumnMap is not { Count: > 0 })
        {
            return null;
        }

        var mappings = new Dictionary<string, string>(PowerShellHelpers.GetHashtableComparer(ColumnMap));
        foreach (DictionaryEntry entry in ColumnMap)
        {
            var source = entry.Key?.ToString();
            var destination = entry.Value?.ToString();
            if (string.IsNullOrWhiteSpace(source))
            {
                throw new PSArgumentException("ColumnMap source column names cannot be null or whitespace.", nameof(ColumnMap));
            }

            if (string.IsNullOrWhiteSpace(destination))
            {
                throw new PSArgumentException("ColumnMap destination column names cannot be null or whitespace.", nameof(ColumnMap));
            }

            mappings[source!] = destination!;
        }

        return mappings;
    }

    private void WriteRowsCopiedProgress(int? totalRows, long rowsCopied)
    {
        var percentComplete = totalRows.GetValueOrDefault() > 0
            ? Math.Min(100, (int)Math.Round(rowsCopied * 100d / totalRows.GetValueOrDefault()))
            : -1;
        var progress = new ProgressRecord(1, $"Writing {DestinationTable}", $"{rowsCopied} row(s) copied")
        {
            PercentComplete = percentComplete
        };

        WriteProgress(progress);
    }

    private void CompleteProgressIfNeeded()
    {
        if (!NotifyAfter.HasValue)
        {
            return;
        }

        WriteProgress(new ProgressRecord(1, $"Writing {DestinationTable}", "Complete")
        {
            RecordType = ProgressRecordType.Completed
        });
    }
}
