namespace DBAClientX.PowerShell;

internal static class DbaXTransactionFactoryOverrides
{
    internal static bool HasOverride(PSCmdlet cmdlet, string providerName)
    {
        var overrides = GetOverrides(cmdlet);
        return overrides.ContainsKey(providerName);
    }

    internal static object CreateClient(PSCmdlet cmdlet, string providerName, Func<object> defaultFactory)
    {
        var overrides = GetOverrides(cmdlet);
        if (!overrides.ContainsKey(providerName))
        {
            return defaultFactory();
        }

        var factory = overrides[providerName];
        var result = factory switch
        {
            ScriptBlock scriptBlock => scriptBlock.InvokeReturnAsIs(),
            _ => factory
        };

        result = NormalizeFactoryResult(result);
        return result ?? throw new PSInvalidOperationException($"Transaction client factory override for {providerName} returned null.");
    }

    private static Hashtable GetOverrides(PSCmdlet cmdlet)
    {
        var value = cmdlet.SessionState.PSVariable.GetValue("DbaXTransactionClientFactoryOverrides");
        if (value is Hashtable hashtable)
        {
            return hashtable;
        }

        var overrides = new Hashtable(StringComparer.OrdinalIgnoreCase);
        cmdlet.SessionState.PSVariable.Set("global:DbaXTransactionClientFactoryOverrides", overrides);
        return overrides;
    }

    private static object? NormalizeFactoryResult(object? result)
    {
        if (result is PSObject)
        {
            return result;
        }

        if (result is IList list && list.Count == 1)
        {
            return list[0];
        }

        return result;
    }

}
