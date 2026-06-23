using System;
using System.Data;
using DBAClientX.SqlServerManagement;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerManagementTests
{
    [Fact]
    public void ParseAgentDateAndTime_ConvertsSqlAgentIntegers()
    {
        Assert.Equal(new DateTime(2026, 6, 23), SqlServerManagementMappers.ParseAgentDate(20260623));
        Assert.Equal(new TimeSpan(14, 5, 7), SqlServerManagementMappers.ParseAgentTime(140507));
        Assert.Null(SqlServerManagementMappers.ParseAgentDate(0));
    }

    [Fact]
    public void MapAgentJob_MapsDefinitionFields()
    {
        Guid jobId = Guid.NewGuid();
        using var reader = ReadSingleRow(
            ("JobId", typeof(Guid), jobId),
            ("Name", typeof(string), "Nightly backup"),
            ("Category", typeof(string), "Database Maintenance"),
            ("OwnerLoginName", typeof(string), "sa"),
            ("Description", typeof(string), "Backs up user databases."),
            ("Enabled", typeof(bool), true),
            ("Created", typeof(DateTime), new DateTime(2026, 1, 2)),
            ("Modified", typeof(DateTime), new DateTime(2026, 1, 3)));

        SqlServerAgentJobInfo job = SqlServerManagementMappers.MapAgentJob(reader);

        Assert.Equal(jobId, job.JobId);
        Assert.Equal("Nightly backup", job.Name);
        Assert.Equal("Database Maintenance", job.Category);
        Assert.True(job.Enabled);
        Assert.Equal(new DateTime(2026, 1, 3), job.Modified);
    }

    [Fact]
    public void MapPrincipal_MapsSidAsHexString()
    {
        using var reader = ReadSingleRow(
            ("Scope", typeof(string), "Server"),
            ("DatabaseName", typeof(string), DBNull.Value),
            ("Name", typeof(string), "app_login"),
            ("Type", typeof(string), "S"),
            ("TypeDescription", typeof(string), "SQL_LOGIN"),
            ("Sid", typeof(byte[]), new byte[] { 0x01, 0xAB }),
            ("DefaultDatabaseName", typeof(string), "AppDb"),
            ("DefaultSchemaName", typeof(string), DBNull.Value),
            ("AuthenticationType", typeof(string), DBNull.Value),
            ("IsDisabled", typeof(bool), false),
            ("IsFixedRole", typeof(bool), false),
            ("Created", typeof(DateTime), new DateTime(2026, 2, 1)),
            ("Modified", typeof(DateTime), new DateTime(2026, 2, 2)));

        SqlServerPrincipalInfo principal = SqlServerManagementMappers.MapPrincipal(reader);

        Assert.Equal("Server", principal.Scope);
        Assert.Equal("app_login", principal.Name);
        Assert.Equal("0x01AB", principal.Sid);
        Assert.False(principal.IsDisabled);
    }

    [Fact]
    public void MapPermission_MapsSecurableAndPrincipals()
    {
        using var reader = ReadSingleRow(
            ("Scope", typeof(string), "Database"),
            ("DatabaseName", typeof(string), "AppDb"),
            ("State", typeof(string), "G"),
            ("StateDescription", typeof(string), "GRANT"),
            ("PermissionName", typeof(string), "SELECT"),
            ("ClassDescription", typeof(string), "OBJECT_OR_COLUMN"),
            ("SecurableSchema", typeof(string), "dbo"),
            ("SecurableName", typeof(string), "Users"),
            ("GranteeName", typeof(string), "app_role"),
            ("GrantorName", typeof(string), "dbo"));

        SqlServerPermissionInfo permission = SqlServerManagementMappers.MapPermission(reader);

        Assert.Equal("Database", permission.Scope);
        Assert.Equal("SELECT", permission.PermissionName);
        Assert.Equal("dbo", permission.SecurableSchema);
        Assert.Equal("Users", permission.SecurableName);
        Assert.Equal("app_role", permission.GranteeName);
    }

    private static DataTableReader ReadSingleRow(params (string Name, Type Type, object Value)[] columns)
    {
        var table = new DataTable();
        foreach ((string name, Type type, _) in columns)
        {
            table.Columns.Add(name, type);
        }

        DataRow row = table.NewRow();
        foreach ((string name, _, object value) in columns)
        {
            row[name] = value;
        }

        table.Rows.Add(row);
        DataTableReader reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }
}
