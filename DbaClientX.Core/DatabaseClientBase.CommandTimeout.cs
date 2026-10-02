using System;
using System.Data.Common;

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    private int _commandTimeout;
    private bool _commandTimeoutConfigured;

    /// <summary>
    /// Gets or sets the command timeout applied to database commands, in seconds.
    /// When explicitly set, <c>0</c> disables the timeout and a positive value
    /// applies a finite timeout. If never set, commands retain the provider default.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public int CommandTimeout
    {
        get
        {
            lock (_syncRoot)
            {
                return _commandTimeout;
            }
        }
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout cannot be negative.");
            }

            lock (_syncRoot)
            {
                _commandTimeout = value;
                _commandTimeoutConfigured = true;
            }
        }
    }

    /// <summary>
    /// Clears an explicit command-timeout override so newly created commands retain
    /// the provider default.
    /// </summary>
    public void ResetCommandTimeout()
    {
        lock (_syncRoot)
        {
            _commandTimeout = 0;
            _commandTimeoutConfigured = false;
        }
    }

    /// <summary>
    /// Applies the explicitly configured timeout to a command. Commands are left
    /// unchanged when no timeout override has been configured.
    /// </summary>
    /// <param name="command">The command to configure.</param>
    protected void ApplyCommandTimeout(DbCommand command)
    {
        if (command == null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        if (TryGetCommandTimeout(out int commandTimeout))
        {
            command.CommandTimeout = commandTimeout;
        }
    }

    /// <summary>
    /// Retrieves an explicit timeout for provider operations that create their own commands,
    /// such as starting a transaction. Returns false when provider defaults remain in effect.
    /// </summary>
    /// <param name="commandTimeout">The configured timeout in seconds, or zero when not configured.</param>
    protected bool TryGetCommandTimeout(out int commandTimeout)
    {
        lock (_syncRoot)
        {
            commandTimeout = _commandTimeout;
            return _commandTimeoutConfigured;
        }
    }
}
