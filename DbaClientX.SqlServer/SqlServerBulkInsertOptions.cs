using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Optional SQL Server bulk-copy behavior for <see cref="SqlServer.BulkInsert(string, System.Data.DataTable, string, bool, int?, int?, SqlServerBulkInsertOptions?)"/>.
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
    /// Number of rows copied between progress notifications.
    /// </summary>
    public int? NotifyAfter { get; set; }

    /// <summary>
    /// Optional callback invoked with the cumulative rows copied when SQL Server raises a rows-copied notification.
    /// </summary>
    public Action<long>? RowsCopied { get; set; }
}
