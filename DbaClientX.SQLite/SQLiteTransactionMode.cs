namespace DBAClientX;

/// <summary>Controls when a managed SQLite transaction acquires its database transaction.</summary>
public enum SQLiteTransactionMode
{
    /// <summary>Starts the transaction immediately and reserves write intent.</summary>
    Immediate,

    /// <summary>Defers the database transaction until the first command and permits read snapshots alongside writers.</summary>
    Deferred
}
