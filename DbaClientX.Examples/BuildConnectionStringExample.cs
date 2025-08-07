using System;

public static class BuildConnectionStringExample
{
    public static void Run()
    {
        var mysql = DBAClientX.MySql.BuildConnectionString("localhost", "mydb", "user", "password");
        var postgres = DBAClientX.PostgreSql.BuildConnectionString("localhost", "mydb", "user", "password");
        var sqlite = DBAClientX.SQLite.BuildConnectionString("data.db");
        var sqlServer = DBAClientX.SqlServer.BuildConnectionString("SQL1", "master", true);
        Console.WriteLine(mysql);
        Console.WriteLine(postgres);
        Console.WriteLine(sqlite);
        Console.WriteLine(sqlServer);
    }
}
