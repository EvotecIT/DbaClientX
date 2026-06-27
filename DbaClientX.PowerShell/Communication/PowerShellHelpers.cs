using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Management.Automation;
using System.Net;
using System.Threading.Tasks;
using DBAClientX.Invoker;

namespace DBAClientX.PowerShell;

/// <summary>
/// Shared helpers for PowerShell cmdlets to reduce duplication and keep behaviors consistent.
/// </summary>
internal static class PowerShellHelpers
{
    internal static readonly IReadOnlyCollection<string> MySqlBulkCopyAllowedUnsupportedOptions = new[] { "AllowLoadLocalInfile", "Allow Load Local Infile" };

    /// <summary>
    /// Converts a Hashtable of parameters (as supplied from PowerShell) into a nullable
    /// dictionary with string keys and nullable object values, filtering out null keys.
    /// Returns null when <paramref name="parameters"/> is null to preserve existing semantics.
    /// </summary>
    internal static IDictionary<string, object?>? ToDictionaryOrNull(Hashtable? parameters)
        => parameters is null
            ? null
            : parameters
                .Cast<DictionaryEntry>()
                .Where(de => de.Key != null)
                .ToDictionary(de => de.Key!.ToString()!, de => (object?)de.Value);

    internal static StringComparer GetHashtableComparer(Hashtable values)
    {
        var keys = values
            .Cast<DictionaryEntry>()
            .Select(static entry => entry.Key?.ToString())
            .Where(static key => !string.IsNullOrEmpty(key))
            .Select(static key => key!)
            .ToArray();
        for (var index = 0; index < keys.Length; index++)
        {
            for (var otherIndex = index + 1; otherIndex < keys.Length; otherIndex++)
            {
                if (!string.Equals(keys[index], keys[otherIndex], StringComparison.Ordinal) &&
                    string.Equals(keys[index], keys[otherIndex], StringComparison.OrdinalIgnoreCase))
                {
                    return StringComparer.Ordinal;
                }
            }
        }

        foreach (DictionaryEntry entry in values)
        {
            var key = entry.Key?.ToString();
            if (string.IsNullOrEmpty(key))
            {
                continue;
            }

            var sourceKey = key!;
            var alternateKey = sourceKey.Any(char.IsLower)
                ? sourceKey.ToUpperInvariant()
                : sourceKey.ToLowerInvariant();
            if (!string.Equals(sourceKey, alternateKey, StringComparison.Ordinal) &&
                values.ContainsKey(alternateKey))
            {
                return StringComparer.OrdinalIgnoreCase;
            }
        }

        return StringComparer.Ordinal;
    }

    internal static IReadOnlyList<DbParameter>? ToDbParameters(
        IDictionary<string, object?>? parameters,
        Func<string, object?, DbParameter> factory)
        => parameters?.Select(kvp => factory(kvp.Key, kvp.Value ?? DBNull.Value)).ToList();

    internal static IReadOnlyList<DbParameter>? ToInMemoryDbParameters(IDictionary<string, object?>? parameters)
        => ToDbParameters(parameters, static (name, value) => new InMemoryDbParameter {
            ParameterName = name,
            Value = value ?? DBNull.Value
        });

    internal static (string Username, string Password, bool IntegratedSecurity) ResolveSqlServerCredential(
        string? username,
        string? password,
        PSCredential? credential)
    {
        if (credential != null)
        {
            NetworkCredential networkCredential = credential.GetNetworkCredential();
            return (networkCredential.UserName ?? string.Empty, networkCredential.Password ?? string.Empty, false);
        }

        var resolvedUsername = username ?? string.Empty;
        var resolvedPassword = password ?? string.Empty;
        return (resolvedUsername, resolvedPassword, string.IsNullOrEmpty(resolvedUsername) && string.IsNullOrEmpty(resolvedPassword));
    }

    internal static (string Username, string Password) ResolveExplicitCredential(
        string? username,
        string? password,
        PSCredential? credential,
        string providerName)
    {
        if (credential != null)
        {
            NetworkCredential networkCredential = credential.GetNetworkCredential();
            return (networkCredential.UserName ?? string.Empty, networkCredential.Password ?? string.Empty);
        }

        var resolvedUsername = username ?? string.Empty;
        var resolvedPassword = password ?? string.Empty;
        if (string.IsNullOrEmpty(resolvedUsername) || string.IsNullOrEmpty(resolvedPassword))
        {
            throw new PSArgumentException($"Provide either -Credential or both -Username and -Password for {providerName} authentication.");
        }

        return (resolvedUsername, resolvedPassword);
    }

    internal static bool TryValidateConnection(
        PSCmdlet cmdlet,
        string providerAlias,
        string connectionString,
        ActionPreference errorAction,
        Action<string>? writeWarning = null,
        Action<ErrorRecord>? throwTerminatingError = null,
        IReadOnlyCollection<string>? allowedUnsupportedOptions = null)
    {
        writeWarning ??= cmdlet is AsyncPSCmdlet asyncCmdlet
            ? asyncCmdlet.WriteWarning
            : cmdlet.WriteWarning;
        throwTerminatingError ??= cmdlet.ThrowTerminatingError;

        var result = DbaConnectionFactory.Validate(providerAlias, connectionString, allowedUnsupportedOptions);
        if (result.IsValid)
        {
            return true;
        }

        var message = DbaConnectionFactory.ToUserMessage(result);
        if (errorAction == ActionPreference.Stop)
        {
            throwTerminatingError(new ErrorRecord(new PSArgumentException(message), result.Code.ToString(), ErrorCategory.InvalidArgument, connectionString));
        }
        else
        {
            writeWarning(message);
        }

        return false;
    }

    internal static bool TryRequireMySqlBulkCopyLocalInfile(
        PSCmdlet cmdlet,
        string connectionString,
        ActionPreference errorAction,
        Action<string>? writeWarning = null,
        Action<ErrorRecord>? throwTerminatingError = null)
    {
        if (HasEnabledMySqlLocalInfileOption(connectionString))
        {
            return true;
        }

        writeWarning ??= cmdlet is AsyncPSCmdlet asyncCmdlet
            ? asyncCmdlet.WriteWarning
            : cmdlet.WriteWarning;
        throwTerminatingError ??= cmdlet.ThrowTerminatingError;

        const string message =
            "MySQL bulk writes require AllowLoadLocalInfile=true or Allow Load Local Infile=true in the connection string. " +
            "Set one of these options before using Write-DbaXTableData with -Provider MySql.";
        if (errorAction == ActionPreference.Stop)
        {
            throwTerminatingError(new ErrorRecord(new PSArgumentException(message), "MySqlLocalInfileRequired", ErrorCategory.InvalidArgument, connectionString));
        }
        else
        {
            writeWarning(message);
        }

        return false;
    }

    internal static bool HasEnabledMySqlLocalInfileOption(string connectionString)
    {
        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };

            return IsEnabledConnectionStringOption(builder, "AllowLoadLocalInfile") ||
                   IsEnabledConnectionStringOption(builder, "Allow Load Local Infile");
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool IsEnabledConnectionStringOption(DbConnectionStringBuilder builder, string key)
    {
        if (!builder.TryGetValue(key, out var value) || value == null)
        {
            return false;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        var text = value.ToString();
        return string.Equals(text, "true", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(text, "1", StringComparison.Ordinal);
    }

    internal static async Task<T?> InvokeOverrideAsync<T>(ScriptBlock overrideBlock, params object?[] args)
    {
        var result = Unwrap(overrideBlock.InvokeReturnAsIs(args));
        switch (result)
        {
            case null:
                return default;
            case Task<T> typedTask:
                return await typedTask.ConfigureAwait(false);
            case Task task:
                await task.ConfigureAwait(false);
                var taskResult = TryGetTaskResult(task);
                return taskResult is T typedTaskResult ? typedTaskResult : (T?)taskResult;
            default:
                return result is T typed ? typed : (T?)result;
        }
    }

    internal static IEnumerable<DataRow> InvokeDataRowOverride(ScriptBlock overrideBlock, params object?[] args)
    {
        var result = Unwrap(overrideBlock.InvokeReturnAsIs(args));
        return result switch
        {
            null => Array.Empty<DataRow>(),
            DataRow row => new[] { row },
            IEnumerable<DataRow> rows => rows,
            IEnumerable enumerable => enumerable.Cast<object?>().Select(Unwrap).OfType<DataRow>(),
            _ => Array.Empty<DataRow>()
        };
    }

    private static object? Unwrap(object? value)
        => value is PSObject psObject ? psObject.BaseObject : value;

    private static object? TryGetTaskResult(Task task)
    {
        var taskType = task.GetType();
        if (!taskType.IsGenericType || taskType.GetGenericTypeDefinition() != typeof(Task<>))
        {
            return null;
        }

        return taskType.GetProperty(nameof(Task<object>.Result))?.GetValue(task);
    }
}

internal sealed class InMemoryDbParameter : DbParameter
{
    private string _parameterName = string.Empty;
    private string _sourceColumn = string.Empty;

    public override DbType DbType { get; set; } = DbType.Object;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
#pragma warning disable CS8765
    public override string ParameterName {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }
    public override string SourceColumn {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }
#pragma warning restore CS8765
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public override void ResetDbType()
    {
        DbType = DbType.Object;
    }
}

internal static class CmdletExtensions
{
    /// <summary>
    /// Reads ErrorActionPreference and optionally overrides it with the explicit -ErrorAction bound parameter.
    /// </summary>
    public static ActionPreference ResolveErrorAction(this PSCmdlet cmdlet)
    {
        var pref = (ActionPreference)cmdlet.SessionState.PSVariable.GetValue("ErrorActionPreference");
        if (cmdlet.MyInvocation.BoundParameters.TryGetValue("ErrorAction", out var value) &&
            value != null && Enum.TryParse(value.ToString(), true, out ActionPreference actionPreference))
        {
            pref = actionPreference;
        }
        return pref;
    }
}
