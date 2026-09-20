using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new OracleConnection(ConnectionString);

    /// <inheritdoc />
    protected override void ConfigureCheckpointCommand(DbCommand command)
        => ((OracleCommand)command).BindByName = true;

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(
        DbConnection connection,
        DbTransaction? transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        var rawSegments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.Oracle);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "Oracle checkpoint destinations require a table name with an optional owner.",
                nameof(definition));
        }

        string Normalize(string segment) => DbaIdentifierPath.IsDelimitedSegment(segment)
            ? DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.Oracle)
            : segment.ToUpperInvariant();

        var owner = rawSegments.Count == 2 ? Normalize(rawSegments[0]) : null;
        var table = Normalize(rawSegments[rawSegments.Count - 1]);
        using var command = new OracleCommand(
            "SELECT OWNER || ':' || OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND OWNER = COALESCE(:owner, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND OBJECT_NAME = :table",
            (OracleConnection)connection)
        {
            Transaction = (OracleTransaction?)transaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        command.Parameters.Add(new OracleParameter("owner", (object?)owner ?? DBNull.Value));
        command.Parameters.Add(new OracleParameter("table", table));
        var identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException(
            $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to an Oracle table.");
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(
        DbConnection connection,
        DbTransaction transaction,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        if (page.Rows.Count == 0)
        {
            return;
        }

        var columns = page.Columns.Cast<DataColumn>().ToArray();
        var parameterNames = columns.Select((_, index) => ":p" + index).ToArray();
        using var command = new OracleCommand(
            $"INSERT INTO {QuotePath(definition.DestinationName)} ({string.Join(", ", columns.Select(column => QuotePath(column.ColumnName)))}) VALUES ({string.Join(", ", parameterNames)})",
            (OracleConnection)connection)
        {
            Transaction = (OracleTransaction)transaction,
            BindByName = true,
            CommandTimeout = options.BulkCopyTimeout ?? CommandTimeout
        };
        for (var index = 0; index < columns.Length; index++)
        {
            command.Parameters.Add(new OracleParameter("p" + index, DBNull.Value));
        }

        foreach (DataRow row in page.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            for (var index = 0; index < columns.Length; index++)
            {
                command.Parameters[index].Value = row[index] ?? DBNull.Value;
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
