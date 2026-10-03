namespace DBAClientX;

/// <summary>Controls whether a client may replay a command after a transient provider failure.</summary>
public enum CommandRetryMode
{
    /// <summary>Executes arbitrary SQL once. Connection establishment may still be retried.</summary>
    Never = 0,

    /// <summary>
    /// Retries commands outside explicit transactions. The caller guarantees that every command
    /// executed by this client is safe to repeat, including every statement in a batch and user callbacks.
    /// </summary>
    ReplaySafe = 1
}
