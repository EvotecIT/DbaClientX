namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Describes a SQL Server instance and authentication settings used by monitoring collectors.
/// </summary>
public sealed class SqlServerMonitoringTarget
{
    /// <summary>Server name, address, or <c>Server\Instance</c> identifier.</summary>
    public string ServerOrInstance { get; set; } = string.Empty;

    /// <summary>Database used for connection-level checks.</summary>
    public string Database { get; set; } = "master";

    /// <summary>When true, uses Windows authentication.</summary>
    public bool IntegratedSecurity { get; set; } = true;

    /// <summary>SQL login name when <see cref="IntegratedSecurity"/> is false.</summary>
    public string? Username { get; set; }

    /// <summary>SQL login password when <see cref="IntegratedSecurity"/> is false.</summary>
    public string? Password { get; set; }

    /// <summary>Optional TCP port to append to the server address.</summary>
    public int? Port { get; set; }

    /// <summary>Trusts the server certificate while still requiring encrypted SQL connections.</summary>
    public bool TrustServerCertificate { get; set; }

    /// <summary>Optional connection timeout in seconds.</summary>
    public int? ConnectTimeoutSeconds { get; set; }

    /// <summary>Application name placed in the SQL connection string for observability.</summary>
    public string? ApplicationName { get; set; } = "DbaClientX.Monitoring";
}
