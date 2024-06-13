using System.Data;
using System.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQL Server
/// </summary>
public class SqlServer {
    public ReturnType ReturnType;
    public int CommandTimeout { get; set; }

    public object SqlQuery(string serverOrInstance, string database, bool integratedSecurity, string query) {
        var connectionString = new SqlConnectionStringBuilder {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
        }.ConnectionString;

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var command = new SqlCommand(query, connection);
        if (command.CommandTimeout > 0) {
            command.CommandTimeout = CommandTimeout;
        }
        var dataAdapter = new SqlDataAdapter(command);
        var dataSet = new System.Data.DataSet();

        dataAdapter.Fill(dataSet);

        if (ReturnType == ReturnType.DataRow || ReturnType == ReturnType.PSObject) {
            if (dataSet.Tables.Count > 0) {
                return dataSet.Tables[0];
            }
        }

        if (ReturnType == ReturnType.DataSet) {
            return dataSet;
        }

        if (ReturnType == ReturnType.DataTable) {
            return dataSet.Tables;
        }

        return null;
    }
}