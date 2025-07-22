using System.Data;
using System.Collections.Generic;

namespace DbaClientX.Tests;

public class TestSqlServer : DBAClientX.SqlServer
{
    public override object? SqlQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        var row = table.NewRow();
        row["Id"] = 1;
        table.Rows.Add(row);
        return table;
    }
}
