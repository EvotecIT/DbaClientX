using System.Data;
using System.Globalization;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    private async Task AddKeysetParametersAsync(SqlConnection connection, SqlCommand command, DbaTableCopyDefinition definition,
        IReadOnlyDictionary<string, object?> parameters, CancellationToken cancellationToken)
    {
        string schemaQuery = $"SELECT TOP (0) {string.Join(", ", definition.OrderByColumns!.Select(QuotePath))} FROM {QuotePath(definition.SourceName)}";
        ReadSessionMetadata? metadata = ReferenceEquals(connection, _readConnection) ? _readMetadata : null;
        if (metadata == null || !metadata.KeyColumnTypes.TryGetValue(schemaQuery, out KeyColumnType[]? types))
        {
            using SqlCommand schemaCommand = CreateSourceCommand(connection, schemaQuery);
            using SqlDataReader schemaReader = await schemaCommand.ExecuteReaderAsync(CommandBehavior.SchemaOnly, cancellationToken).ConfigureAwait(false);
            using DataTable schema = schemaReader.GetSchemaTable() ?? throw new InvalidOperationException("SQL Server did not return key column metadata.");
            types = schema.Rows.Cast<DataRow>().Select(row => new KeyColumnType(
                (SqlDbType)Convert.ToInt32(row["ProviderType"], CultureInfo.InvariantCulture),
                Convert.ToInt32(row["ColumnSize"], CultureInfo.InvariantCulture),
                row["NumericPrecision"] is DBNull ? (byte)0 : Convert.ToByte(row["NumericPrecision"], CultureInfo.InvariantCulture),
                row["NumericScale"] is DBNull ? (byte)0 : Convert.ToByte(row["NumericScale"], CultureInfo.InvariantCulture))).ToArray();
            metadata?.KeyColumnTypes.TryAdd(schemaQuery, types);
        }
        for (int index = 0; index < types.Length; index++)
        {
            string name = "@dbax_key" + index;
            KeyColumnType type = types[index];
            command.Parameters.Add(new SqlParameter(name, type.Type)
            {
                Size = type.Size, Precision = type.Precision, Scale = type.Scale,
                Value = parameters[name] ?? DBNull.Value
            });
        }
    }

    private sealed class KeyColumnType(SqlDbType type, int size, byte precision, byte scale)
    {
        internal SqlDbType Type { get; } = type;
        internal int Size { get; } = size;
        internal byte Precision { get; } = precision;
        internal byte Scale { get; } = scale;
    }
}
