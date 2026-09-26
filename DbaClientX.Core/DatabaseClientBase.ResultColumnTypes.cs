using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>Members <see cref="DataColumn"/> requires from its data type.</summary>
    private const DynamicallyAccessedMemberTypes DataColumnTypeMembers =
        DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>Largest magnitude of <see cref="long"/> that <see cref="double"/> represents exactly (2^53).</summary>
    private const long MaxExactDoubleInteger = 1L << 53;

    /// <summary>
    /// Gets a value indicating whether query materialization adapts <see cref="DataColumn.DataType"/> to the values
    /// actually returned by the provider instead of trusting the field type reported before the first row is read.
    /// </summary>
    /// <remarks>
    /// Dynamically typed providers such as SQLite report field types from the current row, so a single result column can
    /// contain values with different storage classes (for example <c>PRAGMA table_info</c> default values or
    /// <c>SELECT 1 UNION ALL SELECT 'a'</c>). When enabled:
    /// <list type="bullet">
    /// <item><description>columns whose values share one type keep that type;</description></item>
    /// <item><description>a column that has only held nulls adopts the type of the first non-null value;</description></item>
    /// <item><description>integer and floating-point values in one column are materialized as <see cref="double"/> while every integer is exactly representable;</description></item>
    /// <item><description>any other combination of value types widens the column to <see cref="object"/>, preserving each value as returned.</description></item>
    /// </list>
    /// </remarks>
    protected virtual bool AdaptResultColumnTypesToValues => false;

    /// <summary>
    /// Resolves the column type required to store <paramref name="value"/> without losing information.
    /// </summary>
    /// <param name="columnType">The current column type.</param>
    /// <param name="value">The value about to be stored.</param>
    /// <param name="hasObservedValue">Whether the column already stored a non-null value.</param>
    /// <returns>The replacement column type, or <see langword="null"/> when the current type can store the value.</returns>
    [UnconditionalSuppressMessage("Trimming", "IL2073", Justification = "Only reached when AdaptResultColumnTypesToValues is enabled (the SQLite provider), where Microsoft.Data.Sqlite materializes long, double, string or byte[]; DataColumn stores these with built-in storage and does not reflect over their members. Every other result is typeof(object) or typeof(double).")]
    [return: DynamicallyAccessedMembers(DataColumnTypeMembers)]
    private static Type? ResolveAdaptedColumnType(Type columnType, object? value, bool hasObservedValue)
    {
        if (value is null || value is DBNull || columnType == typeof(object))
        {
            return null;
        }

        var valueType = value.GetType();
        if (valueType == columnType)
        {
            return null;
        }

        if (columnType == typeof(double) && value is long integer)
        {
            return IsExactDouble(integer) ? null : typeof(object);
        }

        if (columnType == typeof(long) && value is double)
        {
            return typeof(double);
        }

        return hasObservedValue ? typeof(object) : valueType;
    }

    private static bool IsExactDouble(long value)
        => value >= -MaxExactDoubleInteger && value <= MaxExactDoubleInteger;

    /// <summary>
    /// Adapts populated <paramref name="table"/> columns so the current row values can be stored without losing information.
    /// </summary>
    internal static void AdaptColumnTypesToValues(DataTable table, object?[] values, bool[] observedValues)
    {
        for (var i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (value is null || value is DBNull)
            {
                continue;
            }

            var column = table.Columns[i];
            var adaptedType = ResolveAdaptedColumnType(column.DataType, value, observedValues[i]);
            if (adaptedType == typeof(double) && observedValues[i] && HasInexactDoubleInteger(table, column))
            {
                adaptedType = typeof(object);
            }

            if (adaptedType != null)
            {
                ReplaceColumnType(table, i, adaptedType, copyValues: observedValues[i]);
            }

            observedValues[i] = true;
        }
    }

    /// <summary>
    /// Adapts the column type list used by streamed rows so the current row values can be stored without losing information.
    /// </summary>
    /// <remarks>Rows already streamed keep their own table, so only values of later rows are converted to the adapted types.</remarks>
    /// <returns><see langword="true"/> when at least one column type changed.</returns>
    private static bool AdaptColumnTypesToValues(Type[] columnTypes, object?[] values, bool[] observedValues)
    {
        var changed = false;
        for (var i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (value is null || value is DBNull)
            {
                continue;
            }

            var adaptedType = ResolveAdaptedColumnType(columnTypes[i], value, observedValues[i]);
            if (adaptedType != null)
            {
                columnTypes[i] = adaptedType;
                changed = true;
            }

            observedValues[i] = true;
        }

        return changed;
    }

    private static bool HasInexactDoubleInteger(DataTable table, DataColumn column)
    {
        foreach (DataRow row in table.Rows)
        {
            if (row[column] is long integer && !IsExactDouble(integer))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Replaces a column with one of a different type, preserving its name and ordinal.
    /// </summary>
    /// <param name="table">The table that owns the column.</param>
    /// <param name="ordinal">The column ordinal.</param>
    /// <param name="dataType">The replacement column type.</param>
    /// <param name="copyValues">Whether existing values must be copied; <see langword="false"/> when the column only holds nulls.</param>
    private static void ReplaceColumnType(DataTable table, int ordinal, [DynamicallyAccessedMembers(DataColumnTypeMembers)] Type dataType, bool copyValues)
    {
        var existing = table.Columns[ordinal];
        var columnName = existing.ColumnName;
        var temporaryName = "__DbaClientX_" + Guid.NewGuid().ToString("N");
        var replacement = new DataColumn(temporaryName, dataType);
        table.Columns.Add(replacement);
        if (copyValues)
        {
            foreach (DataRow row in table.Rows)
            {
                row[replacement] = row[existing];
            }
        }

        table.Columns.Remove(existing);
        replacement.SetOrdinal(ordinal);
        replacement.ColumnName = columnName;
    }

    /// <summary>
    /// Creates the detached-row table used by streamed query results.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2062", Justification = "Column types come from DbDataReader.GetFieldType, which carries the DataColumn annotation, or from ResolveAdaptedColumnType, which is annotated; a Type[] cannot carry the annotation itself.")]
    private static DataTable CreateStreamTable(string[] columnNames, Type[] columnTypes)
    {
        var table = new DataTable();
        for (var i = 0; i < columnNames.Length; i++)
        {
            table.Columns.Add(columnNames[i], columnTypes[i]);
        }

        return table;
    }
}
