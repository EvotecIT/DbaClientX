namespace DBAClientX;

/// <summary>
/// Return type for SQL query
/// </summary>
public enum ReturnType {
    DataSet,
    DataTable,
    DataRow,
    /// <summary>
    /// Works only with PowerShell
    /// </summary>
    PSObject
}
