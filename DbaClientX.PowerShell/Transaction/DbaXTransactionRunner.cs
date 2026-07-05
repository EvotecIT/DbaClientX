namespace DBAClientX.PowerShell;

internal static class DbaXTransactionRunner
{
    internal static void Execute(
        PSCmdlet cmdlet,
        string providerName,
        Func<object> clientFactory,
        Action<object> beginTransaction,
        ScriptBlock scriptBlock,
        object[]? argumentList,
        int queryTimeout,
        bool applyQueryTimeout)
    {
        object? client = null;
        try
        {
            client = clientFactory();
            if (applyQueryTimeout)
            {
                SetProperty(client, "CommandTimeout", queryTimeout);
            }

            beginTransaction(client);
            var results = InvokeScriptBlock(cmdlet, scriptBlock, client, argumentList);
            InvokeMethod(client, "Commit");
            foreach (var result in results)
            {
                cmdlet.WriteObject(result);
            }
        }
        catch (Exception ex)
        {
            var rollbackError = TryRollback(client);
            if (rollbackError != null)
            {
                throw new AggregateException(
                    $"Transaction failed and rollback also failed for {providerName}.",
                    ex,
                    rollbackError);
            }

            throw;
        }
        finally
        {
            DisposeClient(client);
        }
    }

    internal static void InvokeBeginTransaction(object client, params object?[] args)
        => InvokeMethod(client, "BeginTransaction", args);

    private static IEnumerable<object?> InvokeScriptBlock(PSCmdlet cmdlet, ScriptBlock scriptBlock, object client, object[]? argumentList)
    {
        var args = new object?[(argumentList?.Length ?? 0) + 1];
        args[0] = client;
        if (argumentList is { Length: > 0 })
        {
            Array.Copy(argumentList, 0, args, 1, argumentList.Length);
        }

        var variables = new List<PSVariable>
        {
            new("ErrorActionPreference", cmdlet.ResolveErrorAction())
        };

        return scriptBlock
            .InvokeWithContext(functionsToDefine: null, variablesToDefine: variables, args)
            .Select(static item => item?.BaseObject);
    }

    private static Exception? TryRollback(object? client)
    {
        if (client == null || !GetBooleanProperty(client, "IsInTransaction"))
        {
            return null;
        }

        try
        {
            InvokeMethod(client, "Rollback");
            return null;
        }
        catch (Exception ex) when (!IsNonRecoverable(ex))
        {
            return ex;
        }
    }

    private static bool IsNonRecoverable(Exception ex)
        => ex is OutOfMemoryException
            or StackOverflowException
            or AccessViolationException
            or AppDomainUnloadedException
            or BadImageFormatException
            or CannotUnloadAppDomainException
            or InvalidProgramException
            or ThreadAbortException;

    private static void DisposeClient(object? client)
    {
        if (client == null)
        {
            return;
        }

        var disposeAsyncMethod = PSObject.AsPSObject(client).Methods["DisposeAsync"];
        if (disposeAsyncMethod != null)
        {
            var result = Unwrap(disposeAsyncMethod.Invoke());
            if (TryWait(result))
            {
                return;
            }
        }

        if (client is IDisposable disposable)
        {
            disposable.Dispose();
            return;
        }

        var disposeMethod = PSObject.AsPSObject(client).Methods["Dispose"];
        disposeMethod?.Invoke();
    }

    private static bool TryWait(object? value)
    {
        if (value == null)
        {
            return false;
        }

        if (value is Task task)
        {
            task.GetAwaiter().GetResult();
            return true;
        }

        var awaiter = value.GetType().GetMethod("GetAwaiter", Type.EmptyTypes)?.Invoke(value, null);
        var getResult = awaiter?.GetType().GetMethod("GetResult", Type.EmptyTypes);
        if (getResult == null)
        {
            return false;
        }

        getResult.Invoke(awaiter, null);
        return true;
    }

    private static void SetProperty(object client, string propertyName, object value)
    {
        var property = PSObject.AsPSObject(client).Properties[propertyName];
        if (property != null && property.IsSettable)
        {
            property.Value = value;
        }
    }

    private static bool GetBooleanProperty(object client, string propertyName)
    {
        var property = PSObject.AsPSObject(client).Properties[propertyName];
        if (property?.Value == null)
        {
            return false;
        }

        return LanguagePrimitives.ConvertTo<bool>(property.Value);
    }

    private static void InvokeMethod(object client, string methodName, params object?[] args)
    {
        var method = PSObject.AsPSObject(client).Methods[methodName];
        if (method == null)
        {
            throw new PSInvalidOperationException($"Transaction client does not expose a {methodName} method.");
        }

        method.Invoke(args);
    }

    private static object? Unwrap(object? value)
        => value is PSObject psObject ? psObject.BaseObject : value;
}
