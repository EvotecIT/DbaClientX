using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Connection-level SQL Server diagnostics collected from a live SQL session.
/// </summary>
public sealed class SqlServerConnectionDiagnostics
{
    /// <summary>SQL target originally requested by the caller.</summary>
    public string Target { get; set; } = string.Empty;

    /// <summary>True when the SQL connection opened successfully.</summary>
    public bool Connected { get; set; }

    /// <summary>True when the lightweight query phase completed successfully.</summary>
    public bool QuerySucceeded { get; set; }

    /// <summary>UTC timestamp when the diagnostic finished.</summary>
    public DateTimeOffset CompletedUtc { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>Elapsed time spent opening the SQL connection.</summary>
    public TimeSpan? ConnectDuration { get; set; }

    /// <summary>Elapsed time spent executing the lightweight diagnostic query.</summary>
    public TimeSpan? QueryDuration { get; set; }

    /// <summary>SQL Server machine name reported by <c>SERVERPROPERTY</c>.</summary>
    public string? MachineName { get; set; }

    /// <summary>SQL Server name reported by <c>SERVERPROPERTY</c>.</summary>
    public string? ServerName { get; set; }

    /// <summary>SQL instance name, or null for the default instance.</summary>
    public string? InstanceName { get; set; }

    /// <summary>Product version reported by SQL Server.</summary>
    public string? ProductVersion { get; set; }

    /// <summary>Product level reported by SQL Server.</summary>
    public string? ProductLevel { get; set; }

    /// <summary>Edition reported by SQL Server.</summary>
    public string? Edition { get; set; }

    /// <summary>Engine edition numeric value reported by SQL Server.</summary>
    public int? EngineEdition { get; set; }

    /// <summary>Original login used by the session.</summary>
    public string? OriginalLogin { get; set; }

    /// <summary>Effective login used by the session.</summary>
    public string? EffectiveLogin { get; set; }

    /// <summary>SQL session id used for the diagnostic query.</summary>
    public int? SessionId { get; set; }

    /// <summary>Local SQL Server network address for the session.</summary>
    public string? LocalNetAddress { get; set; }

    /// <summary>Local SQL Server TCP port for the session.</summary>
    public int? LocalTcpPort { get; set; }

    /// <summary>Client network address observed by SQL Server.</summary>
    public string? ClientNetAddress { get; set; }

    /// <summary>Connection authentication scheme such as KERBEROS, NTLM, or SQL.</summary>
    public string? AuthScheme { get; set; }

    /// <summary>SQL Server encryption option reported for the connection.</summary>
    public string? EncryptOption { get; set; }

    /// <summary>Protocol reported by SQL Server for the connection.</summary>
    public string? ProtocolType { get; set; }

    /// <summary>Normalized error category for failed diagnostics.</summary>
    public string? ErrorCategory { get; set; }

    /// <summary>Human-readable error message for failed diagnostics.</summary>
    public string? ErrorMessage { get; set; }
}
