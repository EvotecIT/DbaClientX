using DBAClientX;
using System.Threading.Tasks;

public static class ConnectionStringExamples
{
    public static async Task RunAsync()
    {
        var sqlServerConnection = SqlServer.BuildConnectionString("SQL1", "master", true);
        using var sqlServer = new SqlServer();
        // Builder parameters are ignored when connectionString is supplied
        await sqlServer.QueryAsync("", "", true, "SELECT 1", connectionString: sqlServerConnection).ConfigureAwait(false);

        var mySqlConnection = MySql.BuildConnectionString("localhost", "mydb", "user", "password");
        using var mySql = new MySql();
        await mySql.QueryAsync("", "", "", "", "SELECT 1", connectionString: mySqlConnection).ConfigureAwait(false);

        var postgreConnection = PostgreSql.BuildConnectionString("localhost", "postgres", "user", "password");
        using var postgre = new PostgreSql();
        await postgre.QueryAsync("", "", "", "", "SELECT 1", connectionString: postgreConnection).ConfigureAwait(false);

        var sqliteConnection = SQLite.BuildConnectionString("data.db");
        using var sqlite = new SQLite();
        await sqlite.QueryAsync("", "SELECT 1", connectionString: sqliteConnection).ConfigureAwait(false);
    }
}
