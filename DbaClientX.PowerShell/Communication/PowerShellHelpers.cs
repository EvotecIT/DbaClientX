using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Management.Automation;
using System.Threading.Tasks;
using DBAClientX.Invoker;

namespace DBAClientX.PowerShell;

/// <summary>
/// Shared helpers for PowerShell cmdlets to reduce duplication and keep behaviors consistent.
/// </summary>
internal static class PowerShellHelpers
{
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

    internal static IReadOnlyList<DbParameter>? ToDbParameters(
        IDictionary<string, object?>? parameters,
        Func<string, object?, DbParameter> factory)
        => parameters?.Select(kvp => factory(kvp.Key, kvp.Value ?? DBNull.Value)).ToList();

    internal static IReadOnlyList<DbParameter>? ToInMemoryDbParameters(IDictionary<string, object?>? parameters)
        => ToDbParameters(parameters, static (name, value) => new InMemoryDbParameter {
            ParameterName = name,
            Value = value ?? DBNull.Value
        });

    internal static bool TryValidateConnection(
        PSCmdlet cmdlet,
        string providerAlias,
        string connectionString,
        ActionPreference errorAction,
        Action<string>? writeWarning = null,
        Action<ErrorRecord>? throwTerminatingError = null)
    {
        writeWarning ??= cmdlet is AsyncPSCmdlet asyncCmdlet
            ? asyncCmdlet.WriteWarning
            : cmdlet.WriteWarning;
        throwTerminatingError ??= cmdlet.ThrowTerminatingError;

        var result = DbaConnectionFactory.Validate(providerAlias, connectionString);
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
                return default;
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
