using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Acquires an exclusive, database-scoped SQL Server application lock on a dedicated session.
    /// The returned session must remain alive for the full protected operation and be disposed
    /// to release the lock. A session lock alone does not fence writes made on other connections.
    /// </summary>
    /// <param name="connectionString">Connection to the database containing the lock namespace.</param>
    /// <param name="resource">Case-sensitive lock resource of at most 255 characters.</param>
    /// <param name="lockTimeoutMilliseconds">Maximum wait for another holder; zero fails immediately.</param>
    /// <param name="cancellationToken">Cancels connection opening or lock acquisition.</param>
    /// <returns>An owned, non-pooled session holding the exclusive lock.</returns>
    public virtual async Task<SqlServerSessionLock> AcquireSessionLockAsync(
        string connectionString,
        string resource,
        int lockTimeoutMilliseconds = 0,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(resource) || resource.Length > 255 ||
            resource.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("A nonempty SQL Server lock resource of at most 255 characters is required.",
                nameof(resource));
        }
        if (lockTimeoutMilliseconds < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(lockTimeoutMilliseconds));
        }
        if (ConnectionOptions?.CompatibilityProfile != SqlServerCompatibilityProfile.Default)
        {
            throw new NotSupportedException("Session application locks require standard SQL Server.");
        }

        SqlConnectionStringBuilder builder = new(connectionString)
        {
            Pooling = false,
            ConnectRetryCount = 0
        };
        SqlConnection connection = CreateConnection(builder.ConnectionString);
        try
        {
            SqlConnectionStringBuilder actual = new(connection.ConnectionString);
            if (actual.Pooling || actual.ConnectRetryCount != 0)
            {
                throw new InvalidOperationException(
                    "A SQL Server session lock requires a non-pooled connection without automatic reconnect.");
            }
            if (!string.Equals(actual.DataSource, builder.DataSource, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(actual.InitialCatalog, builder.InitialCatalog, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "The SQL Server session lock connection must target the requested server and database.");
            }
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            if (!string.Equals(connection.Database, builder.InitialCatalog, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "The SQL Server session lock opened in a different database.");
            }
            using SqlCommand command = connection.CreateCommand();
            // SQL's lock wait and SqlClient's command timeout are independent clocks.
            command.CommandTimeout = (int)Math.Max(30L,
                (lockTimeoutMilliseconds + 999L) / 1000L + 5L);
            command.CommandText = "DECLARE @result int; " +
                "EXEC @result = sys.sp_getapplock @Resource = @resource, " +
                "@LockMode = N'Exclusive', @LockOwner = N'Session', " +
                "@LockTimeout = @timeout; SELECT @result;";
            command.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = resource;
            command.Parameters.Add("@timeout", SqlDbType.Int).Value = lockTimeoutMilliseconds;
            object? value = await command.ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);
            if (value is not int result)
            {
                throw new InvalidOperationException("SQL Server did not return a session-lock acquisition result.");
            }
            if (result < 0)
            {
                throw new InvalidOperationException(
                    $"The exclusive SQL Server session lock was not acquired (result {result}).");
            }
            cancellationToken.ThrowIfCancellationRequested();
            return new SqlServerSessionLock(connection, resource);
        }
        catch
        {
            await DisposeConnectionAsync(connection).ConfigureAwait(false);
            throw;
        }
    }
}
