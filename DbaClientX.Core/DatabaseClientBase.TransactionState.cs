using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>
    /// Detaches the current transaction state and clears the stored references.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <typeparam name="TConnection">The provider connection type.</typeparam>
    /// <param name="transaction">Reference to the stored transaction field.</param>
    /// <param name="connection">Reference to the stored connection field.</param>
    /// <param name="connectionString">Reference to the stored normalized connection string field.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    /// <param name="requireActiveTransaction"><see langword="true"/> to throw when no transaction is active.</param>
    /// <returns>The detached transaction and connection pair.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="requireActiveTransaction"/> is <see langword="true"/> and no transaction is active.</exception>
    protected static (TTransaction? Transaction, TConnection? Connection) DetachTransactionState<TTransaction, TConnection>(
        ref TTransaction? transaction,
        ref TConnection? connection,
        ref string? connectionString,
        ref bool transactionInitializing,
        bool requireActiveTransaction = false)
        where TTransaction : class
        where TConnection : class
    {
        if (requireActiveTransaction && transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }

        var detachedTransaction = transaction;
        var detachedConnection = connection;
        transaction = null;
        connection = null;
        connectionString = null;
        transactionInitializing = false;
        return (detachedTransaction, detachedConnection);
    }

    /// <summary>
    /// Throws when a transaction is already active or currently being initialized.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">The transaction initialization flag.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active or being initialized.</exception>
    protected static void EnsureTransactionStartAllowed<TTransaction>(TTransaction? transaction, bool transactionInitializing)
        where TTransaction : class
    {
        if (transaction != null || transactionInitializing)
        {
            throw new DbaTransactionException("Transaction already started.");
        }
    }

    /// <summary>
    /// Reserves transaction initialization after confirming that no transaction is active.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    protected static void ReserveTransactionStart<TTransaction>(TTransaction? transaction, ref bool transactionInitializing)
        where TTransaction : class
    {
        EnsureTransactionStartAllowed(transaction, transactionInitializing);
        transactionInitializing = true;
    }

    /// <summary>
    /// Stores a successfully started transaction and clears the initialization reservation.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <typeparam name="TConnection">The provider connection type.</typeparam>
    /// <param name="transactionField">Reference to the stored transaction field.</param>
    /// <param name="connectionField">Reference to the stored connection field.</param>
    /// <param name="connectionStringField">Reference to the stored normalized connection string field.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    /// <param name="transaction">The started transaction instance.</param>
    /// <param name="connection">The opened connection instance.</param>
    /// <param name="normalizedConnectionString">The normalized connection string associated with the transaction.</param>
    protected static void StoreStartedTransaction<TTransaction, TConnection>(
        ref TTransaction? transactionField,
        ref TConnection? connectionField,
        ref string? connectionStringField,
        ref bool transactionInitializing,
        TTransaction transaction,
        TConnection connection,
        string normalizedConnectionString)
        where TTransaction : class
        where TConnection : class
    {
        if (transactionField != null)
        {
            transactionInitializing = false;
            throw new DbaTransactionException("Transaction already started.");
        }

        connectionField = connection;
        transactionField = transaction;
        connectionStringField = normalizedConnectionString;
        transactionInitializing = false;
    }

    /// <summary>
    /// Clears the transaction initialization reservation when no active transaction was stored.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    protected static void ReleaseTransactionStartReservationIfNeeded<TTransaction>(TTransaction? transaction, ref bool transactionInitializing)
        where TTransaction : class
    {
        if (transaction == null)
        {
            transactionInitializing = false;
        }
    }
}
