using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter
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
        if (dataType == typeof(Guid)) return OracleDbType.Blob;
        if (dataType == typeof(TimeSpan)) return OracleDbType.IntervalDS;
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

    internal static object GetPageParameterValue(object value)
        => value is ulong unsigned ? Convert.ToDecimal(unsigned)
            : value;

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
                OracleDbType = GetPageParameterType(columns[index].DataType),
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
                    : GetPageParameterValue(value);
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
