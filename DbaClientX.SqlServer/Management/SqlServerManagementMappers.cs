using System;
using System.Data;
using System.Text.RegularExpressions;

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
            StartStepId = GetInt32(record, "StartStepId"),
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
            OnSuccessStepId = GetInt32(record, "OnSuccessStepId"),
            OnFailAction = GetString(record, "OnFailAction"),
            OnFailStepId = GetInt32(record, "OnFailStepId"),
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
            FrequencyRelativeInterval = GetInt32(record, "FrequencyRelativeInterval"),
            FrequencySubdayType = GetInt32(record, "FrequencySubdayType"),
            FrequencySubdayInterval = GetInt32(record, "FrequencySubdayInterval"),
            FrequencyRecurrenceFactor = GetInt32(record, "FrequencyRecurrenceFactor"),
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
            SecurableColumn = GetString(record, "SecurableColumn"),
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

    public static SqlServerDependencyInfo MapDependency(IDataRecord record)
        => new()
        {
            DependencyType = GetString(record, "DependencyType") ?? string.Empty,
            ReferencingSchema = GetString(record, "ReferencingSchema") ?? string.Empty,
            ReferencingName = GetString(record, "ReferencingName") ?? string.Empty,
            ReferencingType = GetString(record, "ReferencingType") ?? string.Empty,
            ReferencedServerName = GetString(record, "ReferencedServerName"),
            ReferencedDatabaseName = GetString(record, "ReferencedDatabaseName"),
            ReferencedSchemaName = GetString(record, "ReferencedSchemaName"),
            ReferencedEntityName = GetString(record, "ReferencedEntityName"),
            ReferencedClassDescription = GetString(record, "ReferencedClassDescription"),
            IsCallerDependent = GetBoolean(record, "IsCallerDependent"),
            IsAmbiguous = GetBoolean(record, "IsAmbiguous")
        };

    public static SqlServerScriptInfo MapScript(IDataRecord record)
        => new()
        {
            ScriptType = GetString(record, "ScriptType") ?? string.Empty,
            SchemaName = GetString(record, "SchemaName") ?? string.Empty,
            ObjectName = GetString(record, "ObjectName") ?? string.Empty,
            ObjectType = GetString(record, "ObjectType") ?? string.Empty,
            Script = NormalizeModuleScript(GetString(record, "Script") ?? string.Empty)
        };

    public static SqlServerTableColumnScriptInfo MapTableScriptColumn(IDataRecord record)
        => new()
        {
            SchemaName = GetString(record, "SchemaName") ?? string.Empty,
            TableName = GetString(record, "TableName") ?? string.Empty,
            ColumnName = GetString(record, "ColumnName") ?? string.Empty,
            Ordinal = GetInt32(record, "Ordinal"),
            DataType = GetString(record, "DataType") ?? string.Empty,
            IsNullable = GetBoolean(record, "IsNullable"),
            IsIdentity = GetBoolean(record, "IsIdentity"),
            IdentitySeed = GetString(record, "IdentitySeed"),
            IdentityIncrement = GetString(record, "IdentityIncrement"),
            IdentityNotForReplication = GetBoolean(record, "IdentityNotForReplication"),
            IsRowGuidColumn = GetBoolean(record, "IsRowGuidColumn"),
            DefaultConstraintName = GetString(record, "DefaultConstraintName"),
            DefaultDefinition = GetString(record, "DefaultDefinition"),
            ComputedDefinition = GetString(record, "ComputedDefinition"),
            IsPersisted = GetBoolean(record, "IsPersisted"),
            GeneratedAlwaysTypeDescription = GetString(record, "GeneratedAlwaysTypeDescription"),
            IsHidden = GetBoolean(record, "IsHidden"),
            IsSparse = GetBoolean(record, "IsSparse"),
            MaskingFunction = GetString(record, "MaskingFunction"),
            TemporalType = GetInt32(record, "TemporalType"),
            HistoryTableSchema = GetString(record, "HistoryTableSchema"),
            HistoryTableName = GetString(record, "HistoryTableName"),
            PrimaryKeyName = GetString(record, "PrimaryKeyName"),
            PrimaryKeyOrdinal = GetNullableInt32(record, "PrimaryKeyOrdinal"),
            PrimaryKeyIndexType = GetString(record, "PrimaryKeyIndexType"),
            PrimaryKeyIsDescending = GetNullableBoolean(record, "PrimaryKeyIsDescending"),
            UniqueConstraintName = GetString(record, "UniqueConstraintName"),
            UniqueConstraintOrdinal = GetNullableInt32(record, "UniqueConstraintOrdinal"),
            UniqueConstraintIndexType = GetString(record, "UniqueConstraintIndexType"),
            UniqueConstraintIsDescending = GetNullableBoolean(record, "UniqueConstraintIsDescending"),
            AdditionalConstraintDefinitions = GetString(record, "AdditionalConstraintDefinitions"),
            PostCreateStatements = GetString(record, "PostCreateStatements")
        };

    internal static string NormalizeModuleScript(string script)
    {
        Match match = Regex.Match(
            script,
            @"\A(?<prefix>(?:\s*SET\s+(?:ANSI_NULLS|QUOTED_IDENTIFIER)\s+(?:ON|OFF)\s*;\s*(?:GO\s*)?)*)\s*ALTER\s+",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        return match.Success
            ? script.Remove(match.Index, match.Length).Insert(match.Index, match.Groups["prefix"].Value + "CREATE OR ALTER ")
            : script;
    }

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

    private static int? GetNullableInt32(IDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? null : Convert.ToInt32(record.GetValue(ordinal));
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
