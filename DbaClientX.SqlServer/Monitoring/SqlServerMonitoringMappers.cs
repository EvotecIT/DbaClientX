using System;
using System.Data;

namespace DBAClientX.SqlServerMonitoring;

internal static class SqlServerMonitoringMappers
{
    public static void PopulateConnectionDiagnostics(IDataRecord record, SqlServerConnectionDiagnostics diagnostic)
    {
        diagnostic.MachineName = GetString(record, "MachineName");
        diagnostic.ServerName = GetString(record, "ServerName");
        diagnostic.InstanceName = GetString(record, "InstanceName");
        diagnostic.ProductVersion = GetString(record, "ProductVersion");
        diagnostic.ProductLevel = GetString(record, "ProductLevel");
        diagnostic.Edition = GetString(record, "Edition");
        diagnostic.EngineEdition = GetInt32(record, "EngineEdition");
        diagnostic.OriginalLogin = GetString(record, "OriginalLogin");
        diagnostic.EffectiveLogin = GetString(record, "EffectiveLogin");
        diagnostic.SessionId = GetInt32(record, "SessionId");
        diagnostic.LocalNetAddress = GetString(record, "LocalNetAddress");
        diagnostic.LocalTcpPort = GetInt32(record, "LocalTcpPort");
        diagnostic.ClientNetAddress = GetString(record, "ClientNetAddress");
        diagnostic.AuthScheme = GetString(record, "AuthScheme");
        diagnostic.EncryptOption = GetString(record, "EncryptOption");
        diagnostic.ProtocolType = GetString(record, "ProtocolType");
    }

    public static SqlServerDatabaseState MapDatabaseState(IDataRecord record)
    {
        return new SqlServerDatabaseState
        {
            DatabaseName = GetString(record, "DatabaseName") ?? string.Empty,
            Status = GetString(record, "Status") ?? string.Empty,
            Access = GetString(record, "AccessMode") ?? string.Empty,
            ReadWrite = GetString(record, "ReadWrite") ?? string.Empty,
            RecoveryModel = GetString(record, "RecoveryModel") ?? string.Empty,
            IsSystemDatabase = GetBoolean(record, "IsSystemDatabase"),
            CreateDate = GetDateTime(record, "CreateDate")
        };
    }

    public static SqlServerBackupFreshness MapBackupFreshness(IDataRecord record, SqlServerMonitoringOptions options)
    {
        var item = new SqlServerBackupFreshness
        {
            DatabaseName = GetString(record, "DatabaseName") ?? string.Empty,
            RecoveryModel = GetString(record, "RecoveryModel") ?? string.Empty,
            DatabaseCreated = GetDateTime(record, "DatabaseCreated"),
            LastFullBackup = GetDateTime(record, "LastFullBackup"),
            LastDifferentialBackup = GetDateTime(record, "LastDifferentialBackup"),
            LastLogBackup = GetDateTime(record, "LastLogBackup")
        };
        DateTime now = DateTime.UtcNow;
        item.SinceFullBackup = SinceUtc(now, item.LastFullBackup);
        item.SinceDifferentialBackup = SinceUtc(now, item.LastDifferentialBackup);
        item.SinceLogBackup = SinceUtc(now, item.LastLogBackup);
        item.RequiresLogBackups = string.Equals(item.RecoveryModel, "FULL", StringComparison.OrdinalIgnoreCase) ||
                                  string.Equals(item.RecoveryModel, "BULK_LOGGED", StringComparison.OrdinalIgnoreCase);
        item.Status = EvaluateBackupStatus(item, options);
        return item;
    }

    public static SqlServerAgentJobHealth MapAgentJobHealth(IDataRecord record)
    {
        return new SqlServerAgentJobHealth
        {
            JobId = GetGuid(record, "JobId") ?? Guid.Empty,
            Name = GetString(record, "Name") ?? string.Empty,
            Category = GetString(record, "Category"),
            OwnerLoginName = GetString(record, "OwnerLoginName"),
            Enabled = GetBoolean(record, "Enabled"),
            IsRunning = GetBoolean(record, "IsRunning"),
            CurrentStartDate = GetDateTime(record, "CurrentStartDate"),
            LastRunDate = GetDateTime(record, "LastRunDate"),
            LastRunOutcome = GetString(record, "LastRunOutcome"),
            LastRunDuration = ParseAgentDuration(GetInt32(record, "LastRunDuration"))
        };
    }

    public static SqlServerWaitStatistic MapWaitStatistic(IDataRecord record)
    {
        return new SqlServerWaitStatistic
        {
            WaitType = GetString(record, "WaitType") ?? string.Empty,
            WaitSeconds = GetDecimal(record, "WaitSeconds"),
            ResourceSeconds = GetDecimal(record, "ResourceSeconds"),
            SignalSeconds = GetDecimal(record, "SignalSeconds"),
            WaitCount = GetInt64(record, "WaitCount"),
            Percentage = GetDecimal(record, "Percentage"),
            AverageWaitSeconds = GetDecimal(record, "AverageWaitSeconds")
        };
    }

    public static SqlServerAvailabilityGroupHealth MapAvailabilityGroupHealth(IDataRecord record)
    {
        return new SqlServerAvailabilityGroupHealth
        {
            AvailabilityGroupName = GetString(record, "AvailabilityGroupName") ?? string.Empty,
            ReplicaServerName = GetString(record, "ReplicaServerName") ?? string.Empty,
            Role = GetString(record, "Role"),
            OperationalState = GetString(record, "OperationalState"),
            ConnectedState = GetString(record, "ConnectedState"),
            SynchronizationHealth = GetString(record, "SynchronizationHealth"),
            DatabaseName = GetString(record, "DatabaseName"),
            DatabaseSynchronizationState = GetString(record, "DatabaseSynchronizationState"),
            DatabaseSynchronizationHealth = GetString(record, "DatabaseSynchronizationHealth"),
            IsSuspended = GetBoolean(record, "IsSuspended"),
            SuspendReason = GetString(record, "SuspendReason")
        };
    }

    public static string EvaluateCheckDbStatus(SqlServerCheckDbFreshness item, SqlServerMonitoringOptions options)
    {
        if (!string.IsNullOrWhiteSpace(item.ErrorMessage))
        {
            return "Unknown";
        }

        if (!item.LastGoodCheckDb.HasValue)
        {
            return item.DatabaseCreated.HasValue && DateTime.UtcNow - item.DatabaseCreated.Value.ToUniversalTime() <= options.MaxCheckDbAge
                ? "New"
                : "Missing";
        }

        return item.SinceLastGoodCheckDb.HasValue && item.SinceLastGoodCheckDb.Value > options.MaxCheckDbAge
            ? "Overdue"
            : "Ok";
    }

    public static string? GetString(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? null : Convert.ToString(record.GetValue(ordinal));
    }

    public static DateTime? GetDateTime(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        if (record.IsDBNull(ordinal)) return null;
        object value = record.GetValue(ordinal);
        if (value is DateTime dt) return dt;
        return DateTime.TryParse(Convert.ToString(value), out DateTime parsed) ? parsed : null;
    }

    private static string EvaluateBackupStatus(SqlServerBackupFreshness item, SqlServerMonitoringOptions options)
    {
        if (!item.LastFullBackup.HasValue)
        {
            return item.DatabaseCreated.HasValue && DateTime.UtcNow - item.DatabaseCreated.Value.ToUniversalTime() <= options.MaxFullBackupAge
                ? "New"
                : "MissingFull";
        }

        if (item.SinceFullBackup.HasValue && item.SinceFullBackup.Value > options.MaxFullBackupAge)
        {
            return "FullOverdue";
        }

        if (item.RequiresLogBackups && (!item.SinceLogBackup.HasValue || item.SinceLogBackup.Value > options.MaxLogBackupAge))
        {
            return "LogOverdue";
        }

        if (item.SinceDifferentialBackup.HasValue && item.SinceDifferentialBackup.Value > options.MaxDifferentialBackupAge)
        {
            return "DifferentialOld";
        }

        return "Ok";
    }

    private static TimeSpan? SinceUtc(DateTime nowUtc, DateTime? value)
    {
        return value.HasValue ? nowUtc - value.Value.ToUniversalTime() : null;
    }

    private static int? GetInt32(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? null : Convert.ToInt32(record.GetValue(ordinal));
    }

    private static long GetInt64(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? 0L : Convert.ToInt64(record.GetValue(ordinal));
    }

    private static decimal GetDecimal(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? 0m : Convert.ToDecimal(record.GetValue(ordinal));
    }

    private static bool GetBoolean(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return !record.IsDBNull(ordinal) && Convert.ToBoolean(record.GetValue(ordinal));
    }

    private static Guid? GetGuid(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        if (record.IsDBNull(ordinal)) return null;
        object value = record.GetValue(ordinal);
        if (value is Guid guid) return guid;
        return Guid.TryParse(Convert.ToString(value), out Guid parsed) ? parsed : null;
    }

    private static TimeSpan? ParseAgentDuration(int? value)
    {
        if (!value.HasValue) return null;
        int raw = value.Value;
        int seconds = raw % 100;
        int minutes = (raw / 100) % 100;
        int hours = raw / 10000;
        return new TimeSpan(hours, minutes, seconds);
    }
}
