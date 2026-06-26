using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Optional SQL Server bulk-copy behavior for <see cref="SqlServer.BulkInsert(string, System.Data.DataTable, string, SqlServerBulkInsertOptions?, bool, int?, int?)"/>.
/// </summary>
public sealed class SqlServerBulkInsertOptions
{
    /// <summary>
    /// Additional <see cref="SqlBulkCopyOptions"/> flags such as <see cref="SqlBulkCopyOptions.TableLock"/> or <see cref="SqlBulkCopyOptions.KeepIdentity"/>.
    /// </summary>
    public SqlBulkCopyOptions BulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default;

    /// <summary>
    /// Optional mapping from source column name to destination column name. When omitted, source and destination column names are matched one-to-one.
    /// </summary>
    public IDictionary<string, string>? ColumnMappings { get; set; }

    /// <summary>
    /// Creates the destination schema and table when they do not already exist, using the incoming <see cref="System.Data.DataTable"/> schema.
    /// </summary>
    /// <remarks>
    /// This option is intended for staging/import tables. It maps common .NET column types to SQL Server types and applies
    /// <see cref="ColumnMappings"/> when choosing destination column names. Existing tables are left unchanged.
    /// </remarks>
    public bool AutoCreateTable { get; set; }

    /// <summary>
    /// Number of rows copied between progress notifications.
    /// </summary>
    public int? NotifyAfter { get; set; }

    /// <summary>
    /// Optional callback invoked with the cumulative rows copied when SQL Server raises a rows-copied notification.
    /// </summary>
    public Action<long>? RowsCopied { get; set; }
}
