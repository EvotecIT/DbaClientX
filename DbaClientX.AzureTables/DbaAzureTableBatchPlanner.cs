using System.Text;

namespace DBAClientX.AzureTables;

/// <summary>Plans transaction batches that stay inside one Azure Table partition.</summary>
public static class DbaAzureTableBatchPlanner
{
    private const int MaximumKeyPayloadBytes = 1024;
    private const int ConservativeEntityPayloadLimit = 1_000_000;
    private const int ConservativeTransactionPayloadLimit = 1_750_000;

    /// <summary>Groups entities by partition and splits each group into batches of at most 100 entities.</summary>
    public static IReadOnlyList<IReadOnlyList<DbaAzureTableEntity>> Plan(
        IEnumerable<DbaAzureTableEntity> entities,
        int batchSize = 100)
    {
        if (entities == null)
        {
            throw new ArgumentNullException(nameof(entities));
        }

        if (batchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Azure Table transaction size must be between 1 and 100.");
        }

        var entityList = entities.ToArray();
        foreach (DbaAzureTableEntity? entity in entityList)
        {
            if (entity == null)
            {
                throw new ArgumentException("Azure Table entity collections cannot contain null values.", nameof(entities));
            }
            ValidateKey(entity.PartitionKey, "PartitionKey", entity);
            ValidateKey(entity.RowKey, "RowKey", entity);
        }

        var batches = new List<IReadOnlyList<DbaAzureTableEntity>>();
        foreach (var partition in entityList.GroupBy(static entity => entity.PartitionKey, StringComparer.Ordinal))
        {
            var batch = new List<DbaAzureTableEntity>(batchSize);
            var batchBytes = 0;
            var rowKeys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var entity in partition)
            {
                var entityBytes = EstimatePayloadBytes(entity);
                if (entityBytes > ConservativeEntityPayloadLimit)
                {
                    throw new ArgumentException(
                        $"Azure Table entity '{entity.PartitionKey}/{entity.RowKey}' exceeds the conservative 1 MB entity payload limit.",
                        nameof(entities));
                }

                if (batch.Count > 0 &&
                    (batch.Count == batchSize || batchBytes + entityBytes > ConservativeTransactionPayloadLimit || rowKeys.Contains(entity.RowKey)))
                {
                    batches.Add(batch);
                    batch = new List<DbaAzureTableEntity>(batchSize);
                    batchBytes = 0;
                    rowKeys.Clear();
                }

                batch.Add(entity);
                batchBytes += entityBytes;
                rowKeys.Add(entity.RowKey);
            }

            if (batch.Count > 0)
            {
                batches.Add(batch);
            }
        }

        return batches;
    }

    private static void ValidateKey(string value, string keyName, DbaAzureTableEntity entity)
    {
        if (Encoding.UTF8.GetByteCount(value) > MaximumKeyPayloadBytes)
        {
            throw new ArgumentException(
                $"Azure Table {keyName} for entity '{entity.PartitionKey}/{entity.RowKey}' exceeds the 1 KiB payload limit.",
                nameof(entity));
        }

        foreach (char character in value)
        {
            if (character is '/' or '\\' or '#' or '?' || character <= '\u001f' || character is >= '\u007f' and <= '\u009f')
            {
                throw new ArgumentException(
                    $"Azure Table {keyName} for entity '{entity.PartitionKey}/{entity.RowKey}' contains a forbidden character.",
                    nameof(entity));
            }
        }
    }

    private static int EstimatePayloadBytes(DbaAzureTableEntity entity)
    {
        long size = 512L + EstimateJsonStringBytes(entity.PartitionKey) + EstimateJsonStringBytes(entity.RowKey);
        foreach (var property in entity.Properties)
        {
            size += 32L + EstimateJsonStringBytes(property.Key) + EstimateValueBytes(property.Value);
            if (size > int.MaxValue)
            {
                return int.MaxValue;
            }
        }

        return (int)size;
    }

    private static long EstimateValueBytes(object? value)
        => value switch
        {
            null => 8,
            string text => EstimateJsonStringBytes(text),
            byte[] bytes => 32L + ((bytes.LongLength + 2L) / 3L * 4L),
            _ => 64
        };

    private static long EstimateJsonStringBytes(string value)
    {
        long size = 2;
        foreach (var character in value)
        {
            size += character switch
            {
                '"' or '\\' => 2,
                < ' ' => 6,
                <= '\u007f' => 1,
                _ => 6
            };
        }

        return size;
    }
}
