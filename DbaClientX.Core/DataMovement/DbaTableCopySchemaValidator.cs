using DBAClientX.Metadata;

namespace DBAClientX.DataMovement;

/// <summary>Checks the writable column contract of a table-copy destination using provider metadata.</summary>
public static class DbaTableCopySchemaValidator
{
    /// <summary>Validates projected names and required columns, with optional identity-preservation enforcement.</summary>
    public static void Validate(string tableName, IReadOnlyCollection<string> projectedColumns, IReadOnlyList<DbaColumnInfo> destinationColumns,
        Func<string, string> normalizeName, bool requirePreservedIdentity, bool keepIdentity)
    {
        if (projectedColumns == null) throw new ArgumentNullException(nameof(projectedColumns));
        if (destinationColumns == null) throw new ArgumentNullException(nameof(destinationColumns));
        if (normalizeName == null) throw new ArgumentNullException(nameof(normalizeName));
        if (destinationColumns.Count == 0) throw new InvalidOperationException($"Destination table '{tableName}' could not be inspected before copying.");
        var columns = destinationColumns.ToDictionary(column => normalizeName(column.Name), StringComparer.Ordinal);
        var supplied = new HashSet<string>(projectedColumns.Select(normalizeName), StringComparer.Ordinal);
        foreach (string name in projectedColumns)
        {
            if (!columns.TryGetValue(normalizeName(name), out DbaColumnInfo? column))
                throw new InvalidOperationException($"Destination table '{tableName}' does not contain copied column '{name}'.");
            if (IsGenerated(column))
                throw new InvalidOperationException($"Destination column '{tableName}.{column.Name}' is generated and cannot be copied. Exclude it from the projection.");
            if (requirePreservedIdentity && column.IsIdentity == true && !keepIdentity)
                throw new InvalidOperationException($"Verified copies of identity column '{tableName}.{column.Name}' require KeepIdentity.");
        }
        foreach (DbaColumnInfo column in destinationColumns)
        {
            if (!supplied.Contains(normalizeName(column.Name)) && column.IsNullable == false && column.IsIdentity != true &&
                !IsGenerated(column) && string.IsNullOrWhiteSpace(column.DefaultExpression))
                throw new InvalidOperationException($"Required destination column '{tableName}.{column.Name}' is not supplied by the copy projection and has no default.");
        }
    }

    private static bool IsGenerated(DbaColumnInfo column)
        => !string.IsNullOrWhiteSpace(column.GeneratedKind) || !string.IsNullOrWhiteSpace(column.GeneratedExpression) ||
           string.Equals(column.DataType, "rowversion", StringComparison.OrdinalIgnoreCase) ||
           (string.Equals(column.DataType, "timestamp", StringComparison.OrdinalIgnoreCase) && column.MaxLength == 8);
}
