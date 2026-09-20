using System.Data;
using DBAClientX.DataMovement;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopyContentValueNormalizer
{
    private NpgsqlConnection? _readConnection;
    private NpgsqlTransaction? _readTransaction;
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
            _readConnection = new NpgsqlConnection(ConnectionString);
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IsolationLevel isolation = ReadConsistency == DbaTableCopyReadConsistency.Snapshot
                ? IsolationLevel.RepeatableRead
                : IsolationLevel.Serializable;
            cancellationToken.ThrowIfCancellationRequested();
            _readTransaction = _readConnection.BeginTransaction(isolation);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private NpgsqlCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."), _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecutePostgreSqlPageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecutePostgreSqlPageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using NpgsqlConnection? owned = _readConnection == null ? new NpgsqlConnection(ConnectionString) : null;
        NpgsqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using NpgsqlCommand command = _readConnection == null
            ? new NpgsqlCommand(query, connection) { CommandTimeout = CommandTimeout }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
            command.Parameters.AddWithValue(parameter.Key, GetPageParameterValue(parameter.Value));
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((NpgsqlCommand)state!).Cancel(), command);
        using NpgsqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        Func<int, long?>? fieldPayloadBytes = maxBytes.HasValue
            ? ordinal => ValidateBoundedFieldType(reader, ordinal)
            : null;
        return await DbaTableCopyPageReader.ReadAsync(
            reader,
            maxBytes,
            fieldPayloadBytes,
            readFieldValue: ordinal => NormalizeProviderValue(reader.GetValue(ordinal)),
            normalizedFieldType: ordinal => GetNormalizedFieldType(reader.GetFieldType(ordinal)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    internal static object NormalizeProviderValue(object value)
    {
#if NET472
        if (value is NpgsqlCidr cidr) return new DbaIpNetwork(cidr.Address, cidr.Netmask);
#else
        if (value is System.Net.IPNetwork network) return new DbaIpNetwork(network.BaseAddress, network.PrefixLength);
#endif
        return value;
    }

    /// <inheritdoc />
    public object? NormalizeContentValue(object value)
    {
        return value switch
        {
            NpgsqlRange<int> range => NormalizeRange(range),
            NpgsqlRange<long> range => NormalizeRange(range),
            NpgsqlRange<decimal> range => NormalizeRange(range),
            NpgsqlRange<DateTime> range => NormalizeRange(range),
#if NET6_0_OR_GREATER
            NpgsqlRange<DateOnly> range => NormalizeRange(range),
#endif
            _ => value
        };
    }

    private static object?[] NormalizeRange<T>(NpgsqlRange<T> range)
    {
        return new object?[]
        {
            "PostgreSQL range",
            range.IsEmpty,
            range.LowerBoundInfinite,
            range.UpperBoundInfinite,
            range.LowerBoundIsInclusive,
            range.UpperBoundIsInclusive,
            range.LowerBoundInfinite ? DBNull.Value : range.LowerBound,
            range.UpperBoundInfinite ? DBNull.Value : range.UpperBound
        };
    }

    internal static Type GetNormalizedFieldType(Type providerType)
    {
#if NET472
        if (providerType == typeof(NpgsqlCidr)) return typeof(DbaIpNetwork);
#else
        if (providerType == typeof(System.Net.IPNetwork)) return typeof(DbaIpNetwork);
#endif
        return providerType;
    }

    internal static object GetPageParameterValue(object? value)
    {
        if (value is not DbaIpNetwork network) return value ?? DBNull.Value;
#if NET472
        return new NpgsqlCidr(network.Address, checked((byte)network.PrefixLength));
#else
        return new System.Net.IPNetwork(network.Address, network.PrefixLength);
#endif
    }

    private static long? ValidateBoundedFieldType(NpgsqlDataReader reader, int ordinal)
    {
        Type type = reader.GetFieldType(ordinal);
        if (type == typeof(string) || type == typeof(byte[]) ||
            type.IsPrimitive || type.IsEnum ||
            type == typeof(decimal) || type == typeof(Guid) ||
            type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan) ||
            type == typeof(System.Net.IPAddress) || type == typeof(System.Net.NetworkInformation.PhysicalAddress))
        {
            return null;
        }
#if NET6_0_OR_GREATER
        if (type == typeof(DateOnly) || type == typeof(TimeOnly)) return null;
#endif
#if NET472
        if (type == typeof(NpgsqlCidr)) return null;
#else
        if (type == typeof(System.Net.IPNetwork)) return null;
#endif
        throw new NotSupportedException(
            $"Bounded PostgreSQL table-copy pages do not materialize variable-size native type '{reader.GetDataTypeName(ordinal)}' ({type.FullName}). Project it to text or binary, or omit MaxPageBytes.");
    }

    private void CloseReadSession()
    {
        try
        {
            _readTransaction?.Dispose();
        }
        finally
        {
            _readTransaction = null;
            _readConnection?.Dispose();
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(PostgreSqlTableCopyAdapter owner) : IDisposable
    {
        private PostgreSqlTableCopyAdapter? _owner = owner;
        public void Dispose()
        {
            PostgreSqlTableCopyAdapter? owner = Interlocked.Exchange(ref _owner, null);
            owner?.CloseReadSession();
        }
    }
}
