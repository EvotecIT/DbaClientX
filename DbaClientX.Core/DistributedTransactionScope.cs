using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DataIsolationLevel = System.Data.IsolationLevel;
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Transactions;
#endif

namespace DBAClientX;

/// <summary>
/// Provides a lightweight abstraction for coordinating multiple connections in a single unit of work.
/// </summary>
/// <remarks>
/// When an ambient <see cref="TransactionScope"/> is present the scope enlists connections into it. When no ambient
/// scope exists the class falls back to creating provider transactions via supplied factories and commits or rolls
/// them together, ensuring callers can orchestrate work across multiple providers without duplicating boilerplate.
/// </remarks>
public sealed class DistributedTransactionScope : IDisposable, IAsyncDisposable
{
    private readonly List<DbTransaction> _localTransactions = new();
    private readonly DataIsolationLevel _isolationLevel;
    private readonly bool _preferAmbient;
    private bool _completed;
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    private readonly TransactionScope? _transactionScope;
#endif

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedTransactionScope"/> class.
    /// </summary>
    /// <param name="isolationLevel">Isolation level applied to provider transactions when no ambient scope exists.</param>
    /// <param name="preferAmbient">When <see langword="true"/> enlists in ambient transactions when present; otherwise uses provider transactions exclusively.</param>
    /// <param name="scopeOption">Ambient scope option used when <see cref="TransactionScope"/> is available.</param>
    /// <param name="asyncFlowOption">Async flow option used when <see cref="TransactionScope"/> is available.</param>
    public DistributedTransactionScope(
        DataIsolationLevel isolationLevel = DataIsolationLevel.ReadCommitted,
        bool preferAmbient = true,
        TransactionScopeOption scopeOption = TransactionScopeOption.Required,
        TransactionScopeAsyncFlowOption asyncFlowOption = TransactionScopeAsyncFlowOption.Enabled)
    {
        _isolationLevel = isolationLevel;
        _preferAmbient = preferAmbient;
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        if (preferAmbient && Transaction.Current == null)
        {
            var transactionOptions = new TransactionOptions
            {
                IsolationLevel = ConvertIsolationLevel(isolationLevel)
            };
            _transactionScope = new TransactionScope(scopeOption, transactionOptions, asyncFlowOption);
        }
#endif
    }

    /// <summary>
    /// Enlists a connection in the distributed transaction, creating a provider transaction when ambient enlistment is not available.
    /// </summary>
    /// <param name="connection">Connection to enlist.</param>
    /// <param name="transactionFactory">Factory invoked to create a provider transaction for the connection.</param>
    /// <returns>The created transaction when a provider transaction was required; otherwise <see langword="null"/>.</returns>
    public DbTransaction? Enlist(DbConnection connection, Func<DbConnection, DataIsolationLevel, DbTransaction> transactionFactory)
    {
        if (TryEnlistAmbient(connection))
        {
            return null;
        }

        var transaction = transactionFactory(connection, _isolationLevel);
        _localTransactions.Add(transaction);
        return transaction;
    }

    /// <summary>
    /// Asynchronously enlists a connection in the distributed transaction, creating a provider transaction when ambient enlistment is not available.
    /// </summary>
    /// <param name="connection">Connection to enlist.</param>
    /// <param name="transactionFactory">Factory invoked to create a provider transaction for the connection.</param>
    /// <param name="cancellationToken">Token used to cancel the enlistment.</param>
    /// <returns>The created transaction when a provider transaction was required; otherwise <see langword="null"/>.</returns>
    public async Task<DbTransaction?> EnlistAsync(
        DbConnection connection,
        Func<DbConnection, DataIsolationLevel, CancellationToken, Task<DbTransaction>> transactionFactory,
        CancellationToken cancellationToken = default)
    {
        if (TryEnlistAmbient(connection))
        {
            return null;
        }

        var transaction = await transactionFactory(connection, _isolationLevel, cancellationToken).ConfigureAwait(false);
        _localTransactions.Add(transaction);
        return transaction;
    }

    /// <summary>
    /// Commits all enlisted transactions and completes the ambient scope when present.
    /// </summary>
    public void Complete()
    {
        _completed = true;
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        _transactionScope?.Complete();
#endif
        foreach (var transaction in _localTransactions)
        {
            transaction.Commit();
        }
    }

    /// <summary>
    /// Asynchronously commits all enlisted transactions and completes the ambient scope when present.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the commit operations.</param>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        _completed = true;
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        _transactionScope?.Complete();
#endif
        foreach (var transaction in _localTransactions)
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            transaction.Commit();
#endif
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_completed)
        {
            RollbackLocalTransactions();
        }
        DisposeLocalTransactions();
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        _transactionScope?.Dispose();
#endif
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            await RollbackLocalTransactionsAsync().ConfigureAwait(false);
        }
        await DisposeLocalTransactionsAsync().ConfigureAwait(false);
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        _transactionScope?.Dispose();
#endif
        GC.SuppressFinalize(this);
    }

    private bool TryEnlistAmbient(DbConnection connection)
    {
#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        if (!_preferAmbient)
        {
            return false;
        }
        var ambient = Transaction.Current;
        if (ambient != null)
        {
            try
            {
                connection.EnlistTransaction(ambient);
                return true;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
            catch (NotSupportedException)
            {
                return false;
            }
            catch (TransactionException)
            {
                return false;
            }
        }
#endif
        return false;
    }

    private void RollbackLocalTransactions()
    {
        foreach (var transaction in _localTransactions)
        {
            transaction.Rollback();
        }
    }

    private async Task RollbackLocalTransactionsAsync()
    {
        foreach (var transaction in _localTransactions)
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await transaction.RollbackAsync().ConfigureAwait(false);
#else
            transaction.Rollback();
#endif
        }
    }

    private void DisposeLocalTransactions()
    {
        foreach (var transaction in _localTransactions)
        {
            transaction.Dispose();
        }
        _localTransactions.Clear();
    }

    private async Task DisposeLocalTransactionsAsync()
    {
        foreach (var transaction in _localTransactions)
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await transaction.DisposeAsync().ConfigureAwait(false);
#else
            transaction.Dispose();
            await Task.Yield();
#endif
        }
        _localTransactions.Clear();
    }

#if NET472 || NET8_0_OR_GREATER || NET5_0_OR_GREATER || NET6_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    private static System.Transactions.IsolationLevel ConvertIsolationLevel(DataIsolationLevel isolationLevel) => isolationLevel switch
    {
        DataIsolationLevel.Chaos => System.Transactions.IsolationLevel.Chaos,
        DataIsolationLevel.ReadCommitted => System.Transactions.IsolationLevel.ReadCommitted,
        DataIsolationLevel.ReadUncommitted => System.Transactions.IsolationLevel.ReadUncommitted,
        DataIsolationLevel.RepeatableRead => System.Transactions.IsolationLevel.RepeatableRead,
        DataIsolationLevel.Serializable => System.Transactions.IsolationLevel.Serializable,
        DataIsolationLevel.Snapshot => System.Transactions.IsolationLevel.Snapshot,
        _ => System.Transactions.IsolationLevel.Unspecified
    };
#endif
}
