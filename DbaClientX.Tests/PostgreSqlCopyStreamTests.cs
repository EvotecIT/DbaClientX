using System;
using System.Data;
using System.Runtime.Serialization;
using HarmonyLib;
using Npgsql;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlCopyStreamTests
{
    private class FakePostgreSql : DBAClientX.PostgreSql
    {
        protected override NpgsqlConnection CreateConnection(string connectionString) => new();
        protected override void OpenConnection(NpgsqlConnection connection) { }
    }
    private static string? _command;
    private static TimeSpan? _timeout;

    private static bool BeginBinaryImportPrefix(string copyFromCommand, ref NpgsqlBinaryImporter __result)
    {
        _command = copyFromCommand;
        __result = (NpgsqlBinaryImporter)FormatterServices.GetUninitializedObject(typeof(NpgsqlBinaryImporter));
        return false;
    }

    private static bool TimeoutSetterPrefix(TimeSpan value)
    {
        _timeout = value;
        return false;
    }

    private static bool Skip() => false;

    [Fact]
    public void BulkInsert_InvokesCopyWithTimeout()
    {
        var harmony = new Harmony(nameof(BulkInsert_InvokesCopyWithTimeout));
        harmony.Patch(
            AccessTools.Method(typeof(NpgsqlConnection), nameof(NpgsqlConnection.BeginBinaryImport), new[] { typeof(string) }),
            prefix: new HarmonyMethod(typeof(PostgreSqlCopyStreamTests), nameof(BeginBinaryImportPrefix)));
        harmony.Patch(
            AccessTools.PropertySetter(typeof(NpgsqlBinaryImporter), nameof(NpgsqlBinaryImporter.Timeout)),
            prefix: new HarmonyMethod(typeof(PostgreSqlCopyStreamTests), nameof(TimeoutSetterPrefix)));
        harmony.Patch(
            AccessTools.Method(typeof(NpgsqlBinaryImporter), nameof(NpgsqlBinaryImporter.Complete)),
            prefix: new HarmonyMethod(typeof(PostgreSqlCopyStreamTests), nameof(Skip)));
        harmony.Patch(
            AccessTools.Method(typeof(NpgsqlBinaryImporter), "Dispose"),
            prefix: new HarmonyMethod(typeof(PostgreSqlCopyStreamTests), nameof(Skip)));

        using var pg = new FakePostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        pg.BulkInsert("h", "d", "u", "p", table, "dest", bulkCopyTimeout: 5);

        harmony.UnpatchAll(harmony.Id);

        Assert.Equal("COPY dest (\"Id\") FROM STDIN (FORMAT BINARY)", _command);
        Assert.Equal(TimeSpan.FromSeconds(5), _timeout);
    }
}
