using System;
using System.Data;

namespace DBAClientX.SqlServerManagement;

internal static class SqlServerManagementMappers
{
    public static SqlServerAgentJobInfo MapAgentJob(IDataRecord record)
        => new()
        {
            JobId = GetGuid(record, "JobId") ?? Guid.Empty,
            Name = GetString(record, "Name") ?? string.Empty,
            Category = GetString(record, "Category"),
            OwnerLoginName = GetString(record, "OwnerLoginName"),
            Description = GetString(record, "Description"),
            Enabled = GetBoolean(record, "Enabled"),
            Created = GetDateTime(record, "Created"),
            Modified = GetDateTime(record, "Modified")
        };

    public static SqlServerAgentJobStepInfo MapAgentJobStep(IDataRecord record)
        => new()
        {
            JobId = GetGuid(record, "JobId") ?? Guid.Empty,
            JobName = GetString(record, "JobName") ?? string.Empty,
            StepId = GetInt32(record, "StepId"),
            StepName = GetString(record, "StepName") ?? string.Empty,
            Subsystem = GetString(record, "Subsystem") ?? string.Empty,
            Command = GetString(record, "Command"),
            DatabaseName = GetString(record, "DatabaseName"),
            OnSuccessAction = GetString(record, "OnSuccessAction"),
            OnFailAction = GetString(record, "OnFailAction"),
            RetryAttempts = GetInt32(record, "RetryAttempts"),
            RetryInterval = GetInt32(record, "RetryInterval")
        };

    public static SqlServerAgentScheduleInfo MapAgentSchedule(IDataRecord record)
        => new()
        {
            JobId = GetGuid(record, "JobId"),
            JobName = GetString(record, "JobName"),
            ScheduleId = GetInt32(record, "ScheduleId"),
            Name = GetString(record, "Name") ?? string.Empty,
            Enabled = GetBoolean(record, "Enabled"),
            FrequencyType = GetInt32(record, "FrequencyType"),
            FrequencyInterval = GetInt32(record, "FrequencyInterval"),
            FrequencySubdayType = GetInt32(record, "FrequencySubdayType"),
            FrequencySubdayInterval = GetInt32(record, "FrequencySubdayInterval"),
            ActiveStartDate = ParseAgentDate(GetInt32(record, "ActiveStartDate")),
            ActiveEndDate = ParseAgentDate(GetInt32(record, "ActiveEndDate")),
            ActiveStartTime = ParseAgentTime(GetInt32(record, "ActiveStartTime")),
            ActiveEndTime = ParseAgentTime(GetInt32(record, "ActiveEndTime"))
        };

    public static SqlServerPrincipalInfo MapPrincipal(IDataRecord record)
        => new()
        {
            Scope = GetString(record, "Scope") ?? string.Empty,
            DatabaseName = GetString(record, "DatabaseName"),
            Name = GetString(record, "Name") ?? string.Empty,
            Type = GetString(record, "Type") ?? string.Empty,
            TypeDescription = GetString(record, "TypeDescription") ?? string.Empty,
            Sid = GetString(record, "Sid"),
            DefaultDatabaseName = GetString(record, "DefaultDatabaseName"),
            DefaultSchemaName = GetString(record, "DefaultSchemaName"),
            AuthenticationType = GetString(record, "AuthenticationType"),
            IsDisabled = GetNullableBoolean(record, "IsDisabled"),
            IsFixedRole = GetNullableBoolean(record, "IsFixedRole"),
            Created = GetDateTime(record, "Created"),
            Modified = GetDateTime(record, "Modified")
        };

    public static SqlServerRoleMembershipInfo MapRoleMembership(IDataRecord record)
        => new()
        {
            Scope = GetString(record, "Scope") ?? string.Empty,
            DatabaseName = GetString(record, "DatabaseName"),
            RoleName = GetString(record, "RoleName") ?? string.Empty,
            RoleTypeDescription = GetString(record, "RoleTypeDescription"),
            MemberName = GetString(record, "MemberName") ?? string.Empty,
            MemberTypeDescription = GetString(record, "MemberTypeDescription")
        };

    public static SqlServerPermissionInfo MapPermission(IDataRecord record)
        => new()
        {
            Scope = GetString(record, "Scope") ?? string.Empty,
            DatabaseName = GetString(record, "DatabaseName"),
            State = GetString(record, "State") ?? string.Empty,
            StateDescription = GetString(record, "StateDescription") ?? string.Empty,
            PermissionName = GetString(record, "PermissionName") ?? string.Empty,
            ClassDescription = GetString(record, "ClassDescription") ?? string.Empty,
            SecurableSchema = GetString(record, "SecurableSchema"),
            SecurableName = GetString(record, "SecurableName"),
            GranteeName = GetString(record, "GranteeName") ?? string.Empty,
            GrantorName = GetString(record, "GrantorName") ?? string.Empty
        };

    public static SqlServerInstancePropertyInfo MapInstanceProperty(IDataRecord record)
        => new()
        {
            Name = GetString(record, "Name") ?? string.Empty,
            Value = GetString(record, "Value")
        };

    public static SqlServerConfigurationInfo MapConfiguration(IDataRecord record)
        => new()
        {
            Name = GetString(record, "Name") ?? string.Empty,
            Value = GetInt32(record, "Value"),
            ValueInUse = GetInt32(record, "ValueInUse"),
            Minimum = GetInt32(record, "Minimum"),
            Maximum = GetInt32(record, "Maximum"),
            IsDynamic = GetBoolean(record, "IsDynamic"),
            IsAdvanced = GetBoolean(record, "IsAdvanced"),
            Description = GetString(record, "Description")
        };

    internal static DateTime? ParseAgentDate(int value)
    {
        if (value <= 0)
        {
            return null;
        }

        int year = value / 10000;
        int month = value / 100 % 100;
        int day = value % 100;
        return year <= 0 || month <= 0 || day <= 0 ? null : new DateTime(year, month, day);
    }

    internal static TimeSpan? ParseAgentTime(int value)
    {
        if (value < 0)
        {
            return null;
        }

        int seconds = value % 100;
        int minutes = value / 100 % 100;
        int hours = value / 10000;
        return new TimeSpan(hours, minutes, seconds);
    }

    private static string? GetString(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        if (record.IsDBNull(ordinal))
        {
            return null;
        }

        object value = record.GetValue(ordinal);
        return value is byte[] bytes ? "0x" + BitConverter.ToString(bytes).Replace("-", string.Empty) : Convert.ToString(value);
    }

    private static DateTime? GetDateTime(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        if (record.IsDBNull(ordinal))
        {
            return null;
        }

        object value = record.GetValue(ordinal);
        return value is DateTime dateTime ? dateTime : DateTime.TryParse(Convert.ToString(value), out DateTime parsed) ? parsed : null;
    }

    private static int GetInt32(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? 0 : Convert.ToInt32(record.GetValue(ordinal));
    }

    private static bool GetBoolean(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return !record.IsDBNull(ordinal) && Convert.ToBoolean(record.GetValue(ordinal));
    }

    private static bool? GetNullableBoolean(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? null : Convert.ToBoolean(record.GetValue(ordinal));
    }

    private static Guid? GetGuid(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        if (record.IsDBNull(ordinal))
        {
            return null;
        }

        object value = record.GetValue(ordinal);
        if (value is Guid guid)
        {
            return guid;
        }

        return Guid.TryParse(Convert.ToString(value), out Guid parsed) ? parsed : null;
    }
}
