using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
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
        if (value is bool boolean && parameterType == OracleDbType.Decimal) return boolean ? 1m : 0m;
        if (value is ulong unsigned) return Convert.ToDecimal(unsigned);
        if (value is Guid guid) return guid.ToByteArray();
#if NET6_0_OR_GREATER
        if (value is DateOnly date) return date.ToDateTime(TimeOnly.MinValue);
        if (value is TimeOnly time) return time.ToTimeSpan();
#endif
        return value;
    }

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
        using var connection = new OracleConnection(ConnectionString);
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
        using var oracle = new Oracle { CommandTimeout = CommandTimeout };
        IReadOnlyList<DbaColumnInfo> destinationColumns = await oracle.GetTableCopyColumnsAsync(
            connection,
            owner,
            table,
            cancellationToken).ConfigureAwait(false);
        string[] projectedColumns = page.Columns.Cast<DataColumn>().Select(column =>
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
        await ResolveDestinationParameterTypesAsync(
            connection,
            null,
            definition,
            page.Columns.Cast<DataColumn>().ToArray(),
            cancellationToken).ConfigureAwait(false);
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
            "SELECT OWNER || ':' || OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND OWNER = COALESCE(:owner, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND OBJECT_NAME = :table",
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
