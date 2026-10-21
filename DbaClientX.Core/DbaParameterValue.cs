using System;
using System.Data;
using System.Data.Common;

namespace DBAClientX;

/// <summary>Supplies a parameter value with explicit portable ADO.NET binding metadata.</summary>
/// <remarks>Use in the existing parameter-value dictionary when stable types or sizes are required. Ordinary values retain provider inference.</remarks>
public sealed class DbaParameterValue
{
    /// <summary>Creates a value with optional binding metadata.</summary>
    /// <param name="value">The input value; null is bound as database null.</param>
    /// <param name="type">Optional type, used unless the separate parameter-type dictionary specifies one.</param>
    /// <param name="size">Optional provider size: characters for strings, bytes for binary values, zero for inference, or -1 for provider-supported unbounded values.</param>
    public DbaParameterValue(object? value, DbType? type = null, int? size = null)
    {
        if (size.HasValue && size.Value < -1) throw new ArgumentOutOfRangeException(nameof(size));
        Value = value;
        Type = type;
        Size = size;
    }

    /// <summary>Gets the input value.</summary>
    public object? Value { get; }

    /// <summary>Gets the optional portable parameter type.</summary>
    public DbType? Type { get; }

    /// <summary>Gets the optional provider parameter size.</summary>
    public int? Size { get; }

    internal static object Unwrap(object? value, out DbaParameterValue? metadata)
    {
        metadata = value as DbaParameterValue;
        return (metadata == null ? value : metadata.Value) ?? DBNull.Value;
    }

    internal static void ApplySize(DbParameter parameter, DbaParameterValue? metadata)
    {
        if (metadata?.Size is int size) parameter.Size = size;
    }
}
