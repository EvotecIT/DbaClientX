using System.Data;

namespace DBAClientX;

/// <summary>
/// Collects streamed <see cref="DataRow"/> values into one <see cref="DataTable"/> whose column types can store every
/// collected value.
/// </summary>
/// <remarks>
/// <para>
/// Streaming APIs such as <c>QueryStreamAsync</c> yield detached rows, and dynamically typed providers such as SQLite
/// start a new source table when a later row needs a wider column type. <see cref="DataTable.ImportRow(DataRow)"/>
/// ignores detached rows, and cloning only the first row's table keeps the first row's types, so neither is a safe way
/// to rebuild a buffered result.
/// </para>
/// <para>
/// The collector copies values instead and widens columns with the same rules as buffered SQLite queries: a column that
/// has only held nulls adopts the runtime type of its first non-null value, <see cref="long"/> and <see cref="double"/>
/// values share a <see cref="double"/> column while every integer is exactly representable, and any other mix of runtime
/// types widens to <see cref="object"/>. Values are never converted to a narrower or different type.
/// </para>
/// <para>
/// Rows are matched by ordinal, so every collected row must come from the same result set. Because widening follows
/// runtime value types, a column declared with a base type (for example a geometry type) whose values are different
/// subclasses is reported as <see cref="object"/>. Rows in the <see cref="DataRowState.Deleted"/> state are skipped.
/// </para>
/// </remarks>
public sealed class DataRowStreamCollector
{
    private readonly string? _tableName;
    private DataTable? _table;
    private bool[] _observedValues = Array.Empty<bool>();

    /// <summary>
    /// Initializes a new collector.
    /// </summary>
    /// <param name="tableName">
    /// Optional name for the collected table. When <see langword="null"/>, the name of the first row's table is kept.
    /// </param>
    public DataRowStreamCollector(string? tableName = null)
    {
        _tableName = tableName;
    }

    /// <summary>
    /// Gets the collected table, or <see langword="null"/> when no row has been added yet.
    /// </summary>
    public DataTable? Table => _table;

    /// <summary>
    /// Gets the number of rows collected so far.
    /// </summary>
    public int RowCount => _table?.Rows.Count ?? 0;

    /// <summary>
    /// Copies the values of <paramref name="row"/> into the collected table, widening column types when needed.
    /// </summary>
    /// <param name="row">The streamed row. Attached and detached rows are both supported; deleted rows are skipped.</param>
    /// <exception cref="ArgumentNullException"><paramref name="row"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The row has a different number of columns than the rows collected before it.</exception>
    public void Add(DataRow row)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        if (row.RowState == DataRowState.Deleted)
        {
            return;
        }

        var values = row.ItemArray;
        if (_table == null)
        {
            _table = CreateTable(row.Table, _tableName ?? row.Table.TableName);
            _observedValues = new bool[_table.Columns.Count];
        }
        else if (values.Length != _table.Columns.Count)
        {
            throw new ArgumentException(
                $"The row has {values.Length} columns but the collected table has {_table.Columns.Count}. Collected rows must come from one result set.",
                nameof(row));
        }

        DatabaseClientBase.AdaptColumnTypesToValues(_table, values, _observedValues);
        _table.Rows.Add(values);
    }

    /// <summary>
    /// Creates an empty table with the source column names and types only. Constraints and expressions are not copied,
    /// because widening replaces columns and collected values always come from the source row.
    /// </summary>
    private static DataTable CreateTable(DataTable source, string tableName)
    {
        var table = new DataTable(tableName)
        {
            Locale = source.Locale,
            CaseSensitive = source.CaseSensitive
        };
        foreach (DataColumn column in source.Columns)
        {
            table.Columns.Add(column.ColumnName, column.DataType);
        }

        return table;
    }
}
