using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter : IDbaTableCopySchemaPreflightDestination, IDbaTableCopySchemaPreflightSessionDestination
{
    internal const string OracleDurableDestinationTableQuery =
        "SELECT 1 FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :table AND TEMPORARY = 'N'";

    internal const string OracleCheckpointDestinationIdentityQuery = @"SELECT obj.OWNER || ':' || obj.OBJECT_ID
FROM ALL_OBJECTS obj
JOIN ALL_TABLES tab ON tab.OWNER = obj.OWNER AND tab.TABLE_NAME = obj.OBJECT_NAME
WHERE obj.OBJECT_TYPE = 'TABLE'
  AND obj.OWNER = COALESCE(:owner, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))
  AND obj.OBJECT_NAME = :table
  AND tab.TEMPORARY = 'N'";

    internal const string OracleCheckpointStorageDurabilityQuery =
        "SELECT TEMPORARY FROM ALL_TABLES WHERE OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND TABLE_NAME = 'DbaX_TableCopyCheckpoints'";

    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new OracleConnection(ConnectionString);

    /// <inheritdoc />
    protected override void ConfigureCheckpointCommand(DbCommand command)
        => ((OracleCommand)command).BindByName = true;

    /// <inheritdoc />
    protected override void ConfigureCheckpointParameter(DbParameter parameter, string name, object? value)
        => ConfigureCheckpointParameter((OracleParameter)parameter, name);

    internal static void ConfigureCheckpointParameter(OracleParameter parameter, string name)
    {
        switch (name)
        {
            case "tableKey":
            case "fingerprint":
            case "sourceHash":
            case "copiedHash":
                parameter.OracleDbType = OracleDbType.Varchar2;
                parameter.Size = 64;
                break;
            case "copyId":
                parameter.OracleDbType = OracleDbType.Varchar2;
                parameter.Size = 128;
                break;
            case "sourceRows":
            case "copiedRows":
                parameter.OracleDbType = OracleDbType.Int64;
                break;
            case "token":
                parameter.OracleDbType = OracleDbType.Clob;
                break;
            case "completed":
                parameter.OracleDbType = OracleDbType.Int16;
                break;
        }
    }

    internal static OracleDbType GetPageParameterType(Type dataType)
    {
        if (dataType == typeof(byte[])) return OracleDbType.Raw;
        if (dataType == typeof(char[])) return OracleDbType.Varchar2;
        if (dataType == typeof(Guid)) return OracleDbType.Raw;
        if (dataType == typeof(TimeSpan)) return OracleDbType.IntervalDS;
        if (dataType == typeof(DbaYearMonthInterval)) return OracleDbType.IntervalYM;
        if (dataType == typeof(DbaArbitraryDecimal)) return OracleDbType.Decimal;
        if (dataType == typeof(DateTimeOffset)) return OracleDbType.TimeStampTZ;
        if (dataType == typeof(OracleBinary)) return OracleDbType.Raw;
        if (dataType == typeof(OracleBlob)) return OracleDbType.Blob;
        if (dataType == typeof(OracleClob)) return OracleDbType.Clob;
        if (dataType == typeof(OracleDate)) return OracleDbType.Date;
        if (dataType == typeof(OracleDecimal)) return OracleDbType.Decimal;
        if (dataType == typeof(OracleIntervalDS)) return OracleDbType.IntervalDS;
        if (dataType == typeof(OracleIntervalYM)) return OracleDbType.IntervalYM;
        if (dataType == typeof(OracleString)) return OracleDbType.Varchar2;
        if (dataType == typeof(OracleTimeStamp)) return OracleDbType.TimeStamp;
        if (dataType == typeof(OracleTimeStampLTZ)) return OracleDbType.TimeStampLTZ;
        if (dataType == typeof(OracleTimeStampTZ)) return OracleDbType.TimeStampTZ;
        if (dataType == typeof(OracleXmlType)) return OracleDbType.XmlType;

        return Type.GetTypeCode(dataType) switch
        {
            TypeCode.Byte => OracleDbType.Byte,
            TypeCode.SByte => OracleDbType.Int16,
            TypeCode.Int16 => OracleDbType.Int16,
            TypeCode.UInt16 => OracleDbType.Int32,
            TypeCode.Int32 => OracleDbType.Int32,
            TypeCode.UInt32 => OracleDbType.Int64,
            TypeCode.Int64 => OracleDbType.Int64,
            TypeCode.UInt64 => OracleDbType.Decimal,
            TypeCode.Decimal => OracleDbType.Decimal,
            TypeCode.Double => OracleDbType.Double,
            TypeCode.Single => OracleDbType.Single,
            TypeCode.Boolean => OracleDbType.Boolean,
            TypeCode.String => OracleDbType.Varchar2,
            TypeCode.Char => OracleDbType.Varchar2,
            TypeCode.DateTime => OracleDbType.TimeStamp,
            _ => throw new NotSupportedException(
                $"Oracle checkpointed table copies do not support DataColumn type '{dataType.FullName}'.")
        };
    }

    internal static OracleDbType GetPageParameterType(Type dataType, string destinationDataType)
    {
        var normalized = destinationDataType.ToUpperInvariant();
#if NET6_0_OR_GREATER
        if (dataType == typeof(DateOnly) &&
            normalized != "DATE" &&
            !(normalized.StartsWith("TIMESTAMP", StringComparison.Ordinal) &&
              !normalized.Contains("TIME ZONE")))
        {
            throw new NotSupportedException(
                $"Oracle destination type '{destinationDataType}' is not compatible with DateOnly values.");
        }
        if (dataType == typeof(TimeOnly) &&
            !normalized.StartsWith("INTERVAL DAY", StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"Oracle destination type '{destinationDataType}' is not compatible with TimeOnly values. Use INTERVAL DAY TO SECOND.");
        }
#endif
        if (normalized.StartsWith("TIMESTAMP", StringComparison.Ordinal))
        {
            if (normalized.Contains("WITH LOCAL TIME ZONE")) return OracleDbType.TimeStampLTZ;
            if (normalized.Contains("WITH TIME ZONE")) return OracleDbType.TimeStampTZ;
            return OracleDbType.TimeStamp;
        }
        if (normalized.StartsWith("INTERVAL DAY", StringComparison.Ordinal)) return OracleDbType.IntervalDS;
        if (normalized.StartsWith("INTERVAL YEAR", StringComparison.Ordinal)) return OracleDbType.IntervalYM;
        return normalized switch
        {
            "NUMBER" or "DECIMAL" or "NUMERIC" or "INTEGER" or "SMALLINT" => OracleDbType.Decimal,
            "FLOAT" or "BINARY_DOUBLE" => OracleDbType.Double,
            "BINARY_FLOAT" => OracleDbType.Single,
            "DATE" => OracleDbType.Date,
            "RAW" => OracleDbType.Raw,
            "BLOB" => OracleDbType.Blob,
            "CLOB" => OracleDbType.Clob,
            "NCLOB" => OracleDbType.NClob,
            "LONG" => OracleDbType.Long,
            "LONG RAW" => OracleDbType.LongRaw,
            _ => GetPageParameterType(dataType)
        };
    }

    internal static object GetPageParameterValue(object value)
        => GetPageParameterValue(value, parameterType: null);

    internal static object GetPageParameterValue(object value, OracleDbType? parameterType)
    {
        if (value is DbaYearMonthInterval interval) return new OracleIntervalYM(interval.TotalMonths);
        if (value is DbaArbitraryDecimal number) return new OracleDecimal(number.CanonicalValue);
        if (value is string text && parameterType == OracleDbType.Decimal) return new OracleDecimal(text);
        if (value is bool boolean && parameterType == OracleDbType.Decimal) return boolean ? 1m : 0m;
        if (value is ulong unsigned) return Convert.ToDecimal(unsigned);
        if (value is Guid guid) return guid.ToByteArray();
#if NET6_0_OR_GREATER
        if (value is DateOnly date) return date.ToDateTime(TimeOnly.MinValue);
        if (value is TimeOnly time) return time.ToTimeSpan();
#endif
        return value;
    }

    internal static void ValidatePageParameterValue(object value, OracleDbType parameterType)
    {
        bool valid = parameterType switch
        {
            OracleDbType.Decimal => IsNumericValue(value) || value is OracleDecimal,
            OracleDbType.Byte or OracleDbType.Int16 or OracleDbType.Int32 or OracleDbType.Int64 or
                OracleDbType.Double or OracleDbType.Single => IsNumericValue(value) || value is OracleDecimal,
            OracleDbType.Boolean => value is bool or OracleBoolean,
            OracleDbType.Date => value is DateTime or OracleDate,
            OracleDbType.TimeStamp => value is DateTime or OracleDate or OracleTimeStamp,
            OracleDbType.TimeStampLTZ => value is DateTime or DateTimeOffset or OracleTimeStampLTZ,
            OracleDbType.TimeStampTZ => value is DateTime or DateTimeOffset or OracleTimeStampTZ,
            OracleDbType.IntervalDS => value is TimeSpan or OracleIntervalDS,
            OracleDbType.IntervalYM => value is OracleIntervalYM,
            OracleDbType.Raw or OracleDbType.LongRaw => value is byte[] or OracleBinary,
            OracleDbType.Blob => value is byte[] or OracleBinary or OracleBlob,
            OracleDbType.Char or OracleDbType.NChar or OracleDbType.Varchar2 or OracleDbType.NVarchar2 or
                OracleDbType.Long or OracleDbType.Clob or OracleDbType.NClob =>
                value is string or char or char[] or OracleString or OracleClob,
            _ => true
        };
        if (!valid)
        {
            throw new InvalidOperationException(
                $"CLR value type '{value.GetType().FullName}' is not compatible with Oracle parameter type '{parameterType}'. Declare an explicit column conversion.");
        }
    }

    private static bool IsNumericValue(object value)
        => Type.GetTypeCode(value.GetType()) is
            TypeCode.Byte or TypeCode.SByte or TypeCode.Int16 or TypeCode.UInt16 or
            TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Int64 or TypeCode.UInt64 or
            TypeCode.Decimal or TypeCode.Double or TypeCode.Single;

    internal static string ResolveDestinationDataType(
        IReadOnlyDictionary<string, string> destinationTypes,
        string columnName)
    {
        bool delimited = DbaIdentifierPath.IsDelimitedSegment(columnName);
        string physicalName = DbaIdentifierPath.UnquoteSegment(columnName, DbaTableCopyProvider.Oracle);
        if (destinationTypes.TryGetValue(physicalName, out string? destinationType))
        {
            return destinationType;
        }

        if (!delimited)
        {
            var match = destinationTypes.FirstOrDefault(pair =>
                string.Equals(pair.Key, physicalName, StringComparison.OrdinalIgnoreCase));
            if (match.Value != null)
            {
                return match.Value;
            }
        }

        throw new InvalidOperationException(
            $"Oracle destination column '{columnName}' could not be resolved for checkpointed binding.");
    }

    internal static void ValidateProjectedIdentityColumns(
        string tableName,
        IReadOnlyCollection<string> projectedColumns,
        IReadOnlyList<DbaColumnInfo> destinationColumns)
    {
        var columns = destinationColumns.ToDictionary(column => column.Name, StringComparer.Ordinal);
        foreach (string name in projectedColumns)
        {
            if (columns.TryGetValue(name, out DbaColumnInfo? column) &&
                column.IsIdentity == true &&
                string.Equals(column.IdentityGeneration, "ALWAYS", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Oracle destination identity column '{tableName}.{column.Name}' is GENERATED ALWAYS and cannot accept copied values. Exclude it from the projection or use a BY DEFAULT identity.");
            }
        }
    }

    private async Task<OracleDbType[]> ResolveDestinationParameterTypesAsync(
        OracleConnection connection,
        OracleTransaction? transaction,
        DbaTableCopyDefinition definition,
        IReadOnlyList<DataColumn> columns,
        CancellationToken cancellationToken)
    {
        var rawSegments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.Oracle);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "Oracle checkpoint destinations require a table name with an optional owner.",
                nameof(definition));
        }

        string Normalize(string segment) => DbaIdentifierPath.IsDelimitedSegment(segment)
            ? DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.Oracle)
            : segment.ToUpperInvariant();

        var owner = rawSegments.Count == 2 ? Normalize(rawSegments[0]) : null;
        var table = Normalize(rawSegments[rawSegments.Count - 1]);
        using var metadata = new OracleCommand(
            "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = COALESCE(:owner, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND TABLE_NAME = :table",
            connection)
        {
            Transaction = transaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        metadata.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2, (object?)owner ?? DBNull.Value, ParameterDirection.Input));
        metadata.Parameters.Add(new OracleParameter("table", OracleDbType.Varchar2, table, ParameterDirection.Input));

        var destinationTypes = new Dictionary<string, string>(StringComparer.Ordinal);
        using OracleDataReader reader = await metadata.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            destinationTypes[reader.GetString(0)] = reader.GetString(1);
        }

        var result = new OracleDbType[columns.Count];
        for (var index = 0; index < columns.Count; index++)
        {
            DataColumn column = columns[index];
            string destinationType = ResolveDestinationDataType(destinationTypes, column.ColumnName);
            result[index] = GetPageParameterType(column.DataType, destinationType);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task ValidateSchemaAsync(
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        await using IDbaTableCopySchemaPreflightSession session = await OpenSchemaPreflightSessionAsync(
            definition,
            page,
            options,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
        DbaTableCopyDefinition definition,
        DataTable firstPage,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var connection = new OracleConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> rawSegments = DbaIdentifierPath.SplitSegments(
                definition.DestinationName,
                DbaTableCopyProvider.Oracle);
            if (rawSegments.Count is < 1 or > 2)
            {
                throw new ArgumentException(
                    "Oracle table-copy destinations support table or owner.table names.",
                    nameof(definition));
            }
            string Normalize(string segment) => DbaIdentifierPath.IsDelimitedSegment(segment)
                ? DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.Oracle)
                : segment.ToUpperInvariant();
            string owner;
            if (rawSegments.Count == 2)
            {
                owner = Normalize(rawSegments[0]);
            }
            else
            {
                using var currentSchema = new OracleCommand(
                    "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual",
                    connection)
                {
                    CommandTimeout = CommandTimeout
                };
                owner = Convert.ToString(await currentSchema.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) ?? "";
                if (string.IsNullOrWhiteSpace(owner))
                {
                    throw new InvalidOperationException("Oracle current schema could not be resolved for destination preflight.");
                }
            }

            string table = Normalize(rawSegments[rawSegments.Count - 1]);
            using (var durableTable = new OracleCommand(OracleDurableDestinationTableQuery, connection)
            {
                BindByName = true,
                CommandTimeout = CommandTimeout
            })
            {
                durableTable.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2, owner, ParameterDirection.Input));
                durableTable.Parameters.Add(new OracleParameter("table", OracleDbType.Varchar2, table, ParameterDirection.Input));
                if (await durableTable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) == null)
                {
                    throw new InvalidOperationException(
                        $"Oracle destination '{definition.DestinationName}' is not a durable table and cannot be used for schema preflight.");
                }
            }

            using var oracle = new Oracle { CommandTimeout = CommandTimeout };
            IReadOnlyList<DbaColumnInfo> destinationColumns = await oracle.GetTableCopyColumnsAsync(
                connection,
                owner,
                table,
                cancellationToken).ConfigureAwait(false);
            string[] projectedColumns = firstPage.Columns.Cast<DataColumn>().Select(column =>
                DbaIdentifierPath.IsDelimitedSegment(column.ColumnName)
                    ? DbaIdentifierPath.UnquoteSegment(column.ColumnName, DbaTableCopyProvider.Oracle)
                    : column.ColumnName.ToUpperInvariant()).ToArray();
            DbaTableCopySchemaValidator.Validate(
                definition.DestinationName,
                projectedColumns,
                destinationColumns,
                static name => name,
                requirePreservedIdentity: false,
                keepIdentity: true);
            ValidateProjectedIdentityColumns(
                definition.DestinationName,
                projectedColumns,
                destinationColumns);
            if (options.ClearDestination)
            {
                ValidateRollbackSafeGeneratorProjection(
                    definition.DestinationName,
                    projectedColumns,
                    destinationColumns);
            }
            var session = new OracleSchemaPreflightSession(
                this,
                connection,
                connection.BeginTransaction(IsolationLevel.ReadCommitted),
                definition,
                options);
            try
            {
                await session.InitializeAsync(firstPage, cancellationToken).ConfigureAwait(false);
                return session;
            }
            catch
            {
                await session.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    internal static void ValidateRollbackSafeGeneratorProjection(
        string tableName,
        IReadOnlyCollection<string> projectedColumns,
        IReadOnlyList<DbaColumnInfo> destinationColumns)
    {
        var supplied = new HashSet<string>(projectedColumns, StringComparer.Ordinal);
        DbaColumnInfo? generator = destinationColumns.FirstOrDefault(column =>
            !supplied.Contains(column.Name) &&
            (column.IsIdentity == true ||
             column.DefaultExpression?.IndexOf("NEXTVAL", StringComparison.OrdinalIgnoreCase) >= 0));
        if (generator == null) return;

        throw new InvalidOperationException(
            $"Oracle destination '{tableName}' omits sequence-backed column '{generator.Name}'. " +
            "ClearDestination cannot safely preflight this projection because sequence advances are not rolled back. " +
            "Project an explicit value for the column or copy without ClearDestination.");
    }

    private sealed class OracleSchemaPreflightSession : IDbaTableCopySchemaPreflightSession
    {
        private readonly OracleTableCopyAdapter _owner;
        private readonly OracleConnection _connection;
        private readonly OracleTransaction _transaction;
        private readonly DbaTableCopyDefinition _definition;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal OracleSchemaPreflightSession(
            OracleTableCopyAdapter owner,
            OracleConnection connection,
            OracleTransaction transaction,
            DbaTableCopyDefinition definition,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definition = definition;
            _options = options;
        }

        internal async Task InitializeAsync(DataTable page, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_options.ClearDestination)
            {
                using var clear = new OracleCommand(
                    $"DELETE FROM {_owner.QuotePath(_definition.DestinationName)}",
                    _connection)
                {
                    Transaction = _transaction,
                    CommandTimeout = _owner.CommandTimeout
                };
                await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await ValidatePageAsync(page, cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(OracleSchemaPreflightSession));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WriteTransactionalPageAsync(
                    _connection,
                    _transaction,
                    _definition,
                    page,
                    _options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"Oracle destination '{_definition.DestinationName}' rejected projected values during schema preflight. No destination rows were changed.",
                    exception);
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed) return default;
            _disposed = true;
            try
            {
                _transaction.Rollback();
            }
            finally
            {
                _transaction.Dispose();
                _connection.Dispose();
            }
            return default;
        }
    }

    /// <inheritdoc />
    protected override async Task ValidateCheckpointSchemaAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        using var command = new OracleCommand(
            OracleCheckpointStorageDurabilityQuery,
            (OracleConnection)connection)
        {
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        object? temporary = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        ValidateCheckpointStorageDurability(temporary as string);
    }

    internal static void ValidateCheckpointStorageDurability(string? temporaryFlag)
    {
        if (!string.Equals(temporaryFlag, "N", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Atomic Oracle checkpoints require DbaX_TableCopyCheckpoints to be a permanent table in the current schema.");
        }
    }

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(
        DbConnection connection,
        DbTransaction? transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        var rawSegments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.Oracle);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "Oracle checkpoint destinations require a table name with an optional owner.",
                nameof(definition));
        }

        string Normalize(string segment) => DbaIdentifierPath.IsDelimitedSegment(segment)
            ? DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.Oracle)
            : segment.ToUpperInvariant();

        var owner = rawSegments.Count == 2 ? Normalize(rawSegments[0]) : null;
        var table = Normalize(rawSegments[rawSegments.Count - 1]);
        using var command = new OracleCommand(
            OracleCheckpointDestinationIdentityQuery,
            (OracleConnection)connection)
        {
            Transaction = (OracleTransaction?)transaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        command.Parameters.Add(new OracleParameter("owner", (object?)owner ?? DBNull.Value));
        command.Parameters.Add(new OracleParameter("table", table));
        var identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException(
            $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to an Oracle table.");
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(
        DbConnection connection,
        DbTransaction transaction,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        if (page.Rows.Count == 0)
        {
            return;
        }

        var columns = page.Columns.Cast<DataColumn>().ToArray();
        var parameterTypes = await ResolveDestinationParameterTypesAsync(
            (OracleConnection)connection,
            (OracleTransaction)transaction,
            definition,
            columns,
            cancellationToken).ConfigureAwait(false);
        var parameterNames = columns.Select((_, index) => ":p" + index).ToArray();
        using var command = new OracleCommand(
            $"INSERT INTO {QuotePath(definition.DestinationName)} ({string.Join(", ", columns.Select(column => QuotePath(column.ColumnName)))}) VALUES ({string.Join(", ", parameterNames)})",
            (OracleConnection)connection)
        {
            Transaction = (OracleTransaction)transaction,
            BindByName = true,
            CommandTimeout = options.BulkCopyTimeout ?? CommandTimeout
        };
        for (var index = 0; index < columns.Length; index++)
        {
            command.Parameters.Add(new OracleParameter
            {
                ParameterName = "p" + index,
                OracleDbType = parameterTypes[index],
                Value = DBNull.Value
            });
        }

        foreach (DataRow row in page.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            for (var index = 0; index < columns.Length; index++)
            {
                var value = row[index];
                command.Parameters[index].Value = value == DBNull.Value
                    ? DBNull.Value
                    : GetPageParameterValue(value, parameterTypes[index]);
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
