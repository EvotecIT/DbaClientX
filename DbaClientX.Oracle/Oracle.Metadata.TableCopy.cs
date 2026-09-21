using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    internal const string OracleTableCopyIdentityColumnsQuery = @"
SELECT column_name, generation_type
FROM all_tab_identity_cols
WHERE owner = :owner AND table_name = :table";

    internal static void AddExactTableCopyMetadataParameters(
        OracleParameterCollection parameters,
        string owner,
        string table)
    {
        parameters.Add(new OracleParameter("schemaNameExact", owner));
        parameters.Add(new OracleParameter("schemaNameNormalized", DBNull.Value));
        parameters.Add(new OracleParameter("tableNameExact", table));
        parameters.Add(new OracleParameter("tableNameNormalized", DBNull.Value));
    }

    internal static void ApplyIdentityGenerations(
        IList<DbaColumnInfo> columns,
        IReadOnlyDictionary<string, string> identityGenerations)
    {
        for (var index = 0; index < columns.Count; index++)
        {
            DbaColumnInfo column = columns[index];
            if (identityGenerations.TryGetValue(column.Name, out string? generation))
            {
                columns[index] = column with { IdentityGeneration = generation };
            }
        }
    }

    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(
        OracleConnection connection,
        string owner,
        string table,
        CancellationToken cancellationToken)
    {
        using var command = new OracleCommand(OracleColumnsQuery, connection)
        {
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        AddExactTableCopyMetadataParameters(command.Parameters, owner, table);

        var columns = new List<DbaColumnInfo>();
        using (OracleDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                columns.Add(MapColumn(reader));
            }
        }

        using var identityCommand = new OracleCommand(OracleTableCopyIdentityColumnsQuery, connection)
        {
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        identityCommand.Parameters.Add(new OracleParameter("owner", owner));
        identityCommand.Parameters.Add(new OracleParameter("table", table));
        var identityGenerations = new Dictionary<string, string>(StringComparer.Ordinal);
        using (OracleDataReader reader = await identityCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                identityGenerations[reader.GetString(0)] = reader.GetString(1);
            }
        }

        ApplyIdentityGenerations(columns, identityGenerations);

        return columns;
    }
}
