using System.Collections;
using System.Data;
using System.Linq;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

/// <summary>
/// Converts common PowerShell pipeline inputs into <see cref="DataTable"/> instances for provider bulk APIs.
/// </summary>
internal static class PowerShellDataTableConverter
{
    internal static object? UnwrapInput(object? item) => Unwrap(item);

    internal static DataTable ToDataTable(IReadOnlyList<object?> input, string? tableName = null)
        => TabularDataTableBuilder.FromItems(input, new TabularDataOptions
        {
            TableName = tableName,
            CopyExistingDataTable = false,
            PreserveNullRows = true,
            ColumnDiscoveryMode = TabularColumnDiscoveryMode.AllRows,
            UnwrapValue = Unwrap,
            ProjectObject = ProjectPowerShellObject
        });

    private static IReadOnlyDictionary<string, object?>? ProjectPowerShellObject(object? item, IReadOnlyList<string>? _)
    {
        if (item is PSObject psObject &&
            psObject.BaseObject is not DataTable &&
            psObject.BaseObject is not DataView &&
            psObject.BaseObject is not IDataReader &&
            psObject.BaseObject is not DataRow &&
            psObject.BaseObject is not DataRowView &&
            psObject.BaseObject is not IDataRecord &&
            psObject.BaseObject is not IDictionary &&
            !TabularDataTableBuilder.IsScalarValue(psObject.BaseObject))
        {
            return GetPSObjectProperties(psObject);
        }

        if (item is not PSObject && item is not IDictionary && !TabularDataTableBuilder.IsScalarValue(item))
        {
            var projected = GetPSObjectProperties(PSObject.AsPSObject(item));
            return projected.Count == 0 ? null : projected;
        }

        return null;
    }

    private static Dictionary<string, object?> GetPSObjectProperties(PSObject psObject)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in psObject.Properties.Where(static property =>
                     (property.MemberType == PSMemberTypes.NoteProperty ||
                      property.MemberType == PSMemberTypes.Property) &&
                     !string.IsNullOrWhiteSpace(property.Name)))
        {
            result[property.Name] = UnwrapValue(property.Value);
        }

        return result;
    }

    private static object? Unwrap(object? item)
    {
        if (item is not PSObject psObject)
        {
            return item;
        }

        return psObject.BaseObject is DataTable or DataView or IDataReader or DataRow or DataRowView or IDataRecord or IDictionary or IEnumerable ||
               TabularDataTableBuilder.IsScalarValue(psObject.BaseObject)
            ? psObject.BaseObject
            : psObject;
    }

    private static object? UnwrapValue(object? value)
        => value is PSObject psObject ? psObject.BaseObject : value;
}
