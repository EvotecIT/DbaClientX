using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Management.Automation;

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
