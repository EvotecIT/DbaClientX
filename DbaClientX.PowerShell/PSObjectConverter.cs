using System.Runtime.CompilerServices;

namespace DBAClientX.PowerShell;

/// <summary>
/// Provides helpers for converting common data structures to <see cref="PSObject"/> instances.
/// </summary>
public static class PSObjectConverter
{
    private static readonly ConditionalWeakTable<DataTable, PSNoteProperty[]> _psNotePropertyCache = new();

    /// <summary>
    /// Converts a <see cref="DataRow"/> into a PowerShell <see cref="PSObject"/> with note properties matching the row's columns.
    /// </summary>
    /// <param name="row">The data row to convert.</param>
    /// <returns>A <see cref="PSObject"/> representing the provided data row.</returns>
    public static PSObject DataRowToPSObject(DataRow row)
    {
        PSObject psObject = new PSObject();

        if (row != null && (row.RowState & DataRowState.Detached) != DataRowState.Detached)
        {
            var table = row.Table;
            if (!_psNotePropertyCache.TryGetValue(table, out var propertyTemplates))
            {
                propertyTemplates = new PSNoteProperty[table.Columns.Count];
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    propertyTemplates[i] = new PSNoteProperty(table.Columns[i].ColumnName, null);
                }
                _psNotePropertyCache.Add(table, propertyTemplates);
            }

            for (int i = 0; i < propertyTemplates.Length; i++)
            {
                var prop = (PSNoteProperty)propertyTemplates[i].Copy();
                if (!row.IsNull(i))
                {
                    prop.Value = row[i];
                }
                psObject.Properties.Add(prop);
            }
        }

        return psObject;
    }
}

