namespace DBAClientX.PowerShell;

/// <summary>Collects SQLite file and database diagnostics through the DbaClientX SQLite provider.</summary>
/// <example>
/// <summary>Inspect a SQLite database.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXSQLiteDiagnostics -Database .\app.db</code>
/// <para>Returns file, integrity, journal, and SQLite version diagnostics.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXSQLiteDiagnostics")]
[CmdletBinding()]
public sealed class CmdletGetDbaXSQLiteDiagnostics : AsyncPSCmdlet
{
    /// <summary>SQLite database path or SQLite connection string.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [Alias("Path", "ConnectionString")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Optional busy timeout in milliseconds.</summary>
    [Parameter(Mandatory = false)]
    public int? BusyTimeoutMs { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        if (BusyTimeoutMs.HasValue && BusyTimeoutMs.Value < 0)
        {
            throw new PSArgumentException("BusyTimeoutMs cannot be negative.", nameof(BusyTimeoutMs));
        }

        using var client = new DBAClientX.SQLite();
        WriteObject(await client.CollectDiagnosticsAsync(
            DbaXProviderHelpers.GetSQLiteDatabase(Database),
            CancelToken,
            BusyTimeoutMs).ConfigureAwait(false));
    }
}
