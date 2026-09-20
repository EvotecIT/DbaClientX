using System.Data;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter
{
    private OracleConnection? _readConnection;
    private OracleTransaction? _readTransaction;
    private int _readSessionActive;

    /// <summary>Consistency used for an engine-owned source read session.</summary>
    public DbaTableCopyReadConsistency ReadConsistency { get; set; }

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged) return null;
        if (ReadConsistency is not (DbaTableCopyReadConsistency.Snapshot or DbaTableCopyReadConsistency.Serializable))
            throw new ArgumentOutOfRangeException(nameof(ReadConsistency));
        if (Interlocked.CompareExchange(ref _readSessionActive, 1, 0) != 0)
            throw new InvalidOperationException("A table-copy read session is already active on this adapter.");
        try
        {
            _readConnection = new OracleConnection(ConnectionString);
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            _readTransaction = _readConnection.BeginTransaction(IsolationLevel.Serializable);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private OracleCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."))
        {
            Transaction = _readTransaction,
            CommandTimeout = CommandTimeout,
            BindByName = true
        };

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteOraclePageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecuteOraclePageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using OracleConnection? owned = _readConnection == null ? new OracleConnection(ConnectionString) : null;
        OracleConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using OracleCommand command = _readConnection == null
            ? new OracleCommand(query, connection) { CommandTimeout = CommandTimeout, BindByName = true }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
        {
            string name = parameter.Key.TrimStart(':', '@');
            object value = parameter.Value == null
                ? DBNull.Value
                : GetPageParameterValue(parameter.Value);
            command.Parameters.Add(new OracleParameter(name, value));
        }
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((OracleCommand)state!).Cancel(), command);
        using OracleDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        Func<int, long?>? fieldPayloadBytes = maxBytes.HasValue
            ? ordinal => ValidateBoundedFieldType(reader.GetFieldType(ordinal), reader.GetDataTypeName(ordinal))
            : null;
        return await DbaTableCopyPageReader.ReadAsync(
            reader,
            maxBytes,
            fieldPayloadBytes,
            readFieldValue: ordinal => ReadProviderValue(reader, ordinal),
            normalizedFieldType: ordinal => GetNormalizedFieldType(reader.GetFieldType(ordinal), reader.GetDataTypeName(ordinal)),
            cancellationToken).ConfigureAwait(false);
    }

    internal static object ReadProviderValue(OracleDataReader reader, int ordinal)
    {
        string dataTypeName = reader.GetDataTypeName(ordinal);
        if (IsOracleNumber(dataTypeName))
        {
            return NormalizeProviderValue(reader.GetOracleDecimal(ordinal));
        }
        return NormalizeProviderValue(reader.GetValue(ordinal));
    }

    internal static object NormalizeProviderValue(object value)
    {
        if (value is OracleBlob blob)
        {
            try { return blob.IsNull ? DBNull.Value : blob.Value; }
            finally { blob.Dispose(); }
        }
        if (value is OracleClob clob)
        {
            try { return clob.IsNull ? DBNull.Value : clob.Value; }
            finally { clob.Dispose(); }
        }
        if (value is OracleXmlType xml)
        {
            try { return xml.IsNull ? DBNull.Value : xml.Value; }
            finally { xml.Dispose(); }
        }

        return value switch
        {
            OracleIntervalYM interval => interval.IsNull
                ? DBNull.Value
                : new DbaYearMonthInterval(interval.Value),
            OracleIntervalDS interval => interval.IsNull ? DBNull.Value : NormalizeDaySecondInterval(interval),
            OracleBinary binary => binary.IsNull ? DBNull.Value : binary.Value,
            OracleBoolean boolean => boolean.IsNull ? DBNull.Value : boolean.Value,
            OracleDecimal number => NormalizeOracleNumber(number),
            OracleDate date => date.IsNull
                ? DBNull.Value
                : DateTime.SpecifyKind(date.Value, DateTimeKind.Unspecified),
            OracleString text => text.IsNull ? DBNull.Value : text.Value,
            OracleTimeStamp timestamp => timestamp.IsNull
                ? DBNull.Value
                : NormalizeTimestamp(timestamp),
            OracleTimeStampLTZ timestamp => timestamp.IsNull
                ? DBNull.Value
                : NormalizeTimestamp(timestamp.ToUniversalTime()),
            OracleTimeStampTZ timestamp => timestamp.IsNull
                ? DBNull.Value
                : NormalizeTimestamp(timestamp),
            _ => value
        };
    }

    internal static Type GetNormalizedFieldType(Type providerType)
        => GetNormalizedFieldType(providerType, dataTypeName: null);

    internal static Type GetNormalizedFieldType(Type providerType, string? dataTypeName)
    {
        if (dataTypeName != null && IsOracleNumber(dataTypeName)) return typeof(object);
        if (providerType == typeof(OracleIntervalYM)) return typeof(DbaYearMonthInterval);
        if (providerType == typeof(OracleIntervalDS)) return typeof(TimeSpan);
        if (providerType == typeof(OracleBinary) || providerType == typeof(OracleBlob)) return typeof(byte[]);
        if (providerType == typeof(OracleBoolean)) return typeof(bool);
        if (providerType == typeof(OracleDecimal)) return typeof(object);
        if (providerType == typeof(OracleDate) || providerType == typeof(OracleTimeStamp)) return typeof(DateTime);
        if (providerType == typeof(OracleTimeStampLTZ) || providerType == typeof(OracleTimeStampTZ)) return typeof(DateTimeOffset);
        if (providerType == typeof(OracleString) || providerType == typeof(OracleClob) || providerType == typeof(OracleXmlType)) return typeof(string);
        return providerType;
    }

    private static bool IsOracleNumber(string dataTypeName)
    {
        string normalized = dataTypeName.Trim().ToUpperInvariant();
        return normalized.StartsWith("NUMBER", StringComparison.Ordinal) ||
               normalized.StartsWith("FLOAT", StringComparison.Ordinal) ||
               normalized.StartsWith("DECIMAL", StringComparison.Ordinal) ||
               normalized.StartsWith("NUMERIC", StringComparison.Ordinal) ||
               normalized == "INTEGER" ||
               normalized == "SMALLINT";
    }

    private static object NormalizeOracleNumber(OracleDecimal number)
    {
        if (number.IsNull) return DBNull.Value;
        try
        {
            return number.Value;
        }
        catch (Exception exception) when (
            exception is OverflowException or InvalidCastException ||
            exception.InnerException is OverflowException)
        {
            return new DbaArbitraryDecimal(number.ToString());
        }
    }

    internal static long? ValidateBoundedFieldType(Type providerType, string dataTypeName)
    {
        if (providerType == typeof(OracleBlob) || providerType == typeof(OracleClob) || providerType == typeof(OracleXmlType))
        {
            throw new NotSupportedException(
                $"Bounded Oracle table-copy pages do not materialize provider-native type '{dataTypeName}' ({providerType.FullName}). Project it to text or binary, or omit MaxPageBytes.");
        }
        return null;
    }

    private static TimeSpan NormalizeDaySecondInterval(OracleIntervalDS interval)
    {
        ValidateOracleNanosecondResolution(interval.Nanoseconds, "INTERVAL DAY TO SECOND");
        try
        {
            return interval.Value;
        }
        catch (OverflowException exception)
        {
            throw new NotSupportedException(
                "Oracle INTERVAL DAY TO SECOND value exceeds the CLR TimeSpan range and cannot be copied losslessly.",
                exception);
        }
    }

    private static DateTime NormalizeTimestamp(OracleTimeStamp timestamp)
    {
        ValidateOracleNanosecondResolution(timestamp.Nanosecond, "TIMESTAMP");
        return DateTime.SpecifyKind(timestamp.Value, DateTimeKind.Unspecified);
    }

    private static DateTimeOffset NormalizeTimestamp(OracleTimeStampTZ timestamp)
    {
        ValidateOracleNanosecondResolution(timestamp.Nanosecond, "TIMESTAMP WITH TIME ZONE");
        if (IsOracleTimeZoneRegion(timestamp.TimeZone))
        {
            throw new NotSupportedException(
                $"Oracle TIMESTAMP WITH TIME ZONE region '{timestamp.TimeZone}' cannot be represented losslessly by CLR DateTimeOffset values.");
        }
        return new DateTimeOffset(
            DateTime.SpecifyKind(timestamp.Value, DateTimeKind.Unspecified),
            timestamp.GetTimeZoneOffset());
    }

    internal static bool IsOracleTimeZoneRegion(string timeZone)
        => !string.IsNullOrWhiteSpace(timeZone) && timeZone[0] is not ('+' or '-');

    private static void ValidateOracleNanosecondResolution(int nanoseconds, string dataType)
    {
        if (nanoseconds % 100 == 0) return;
        throw new NotSupportedException(
            $"Oracle {dataType} value contains fractional seconds below the 100-nanosecond CLR tick resolution and cannot be copied losslessly.");
    }

    private void CloseReadSession()
    {
        try { _readTransaction?.Dispose(); }
        finally
        {
            _readTransaction = null;
            _readConnection?.Dispose();
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(OracleTableCopyAdapter owner) : IDisposable
    {
        private OracleTableCopyAdapter? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.CloseReadSession();
    }
}
