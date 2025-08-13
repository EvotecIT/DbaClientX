using System;

public static class BuildConnectionStringExample
{
    public static void Run()
    {
        var mysql = DBAClientX.MySql.BuildConnectionString("localhost", "mydb", "user", "password", port: 3307, ssl: true);
        var postgres = DBAClientX.PostgreSql.BuildConnectionString("localhost", "mydb", "user", "password", port: 5433, ssl: true);
        var sqlite = DBAClientX.SQLite.BuildConnectionString("data.db");
        var sqlServer = DBAClientX.SqlServer.BuildConnectionString("SQL1", "master", true, port: 1444, ssl: true);
        Console.WriteLine(mysql);
        Console.WriteLine(postgres);
        Console.WriteLine(sqlite);
        Console.WriteLine(sqlServer);
    }
}
