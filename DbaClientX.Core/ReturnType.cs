namespace DBAClientX;

/// <summary>
/// Defines the shape of the data returned by query execution helpers.
/// </summary>
public enum ReturnType
{
    /// <summary>
    /// Materialize the result as a <see cref="System.Data.DataSet"/> containing all result sets.
    /// </summary>
    DataSet,

    /// <summary>
    /// Materialize the first result set as a <see cref="System.Data.DataTable"/>.
    /// </summary>
    DataTable,

    /// <summary>
    /// Materialize the first row of the first result set as a <see cref="System.Data.DataRow"/>.
    /// </summary>
    DataRow,

    /// <summary>
    /// Materialize the first result set as a <see cref="System.Data.DataTable"/> optimized for PowerShell interoperability.
    /// </summary>
    PSObject
}
