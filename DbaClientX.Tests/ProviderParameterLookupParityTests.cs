using System.Collections.Generic;
using System.Data;
using DBAClientX;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class ProviderParameterLookupParityTests
{
    private sealed class SqlServerParameterProbe : DBAClientX.SqlServer
    {
        public SqlParameter CaptureMetadata(string? value)
        {
            using var command = new SqlCommand();
            AddParameters(command,
                new Dictionary<string, object?> { ["@VALUE"] = new DbaParameterValue(value, DbType.AnsiString, 4000) },
                ConvertParameterTypes(new Dictionary<string, SqlDbType> { ["@value"] = SqlDbType.NVarChar }),
                new Dictionary<string, ParameterDirection> { ["@value"] = ParameterDirection.InputOutput });
            return (SqlParameter)command.Parameters[0];
        }

        public SqlParameter Capture()
        {
            using var command = new SqlCommand();
            AddParameters(
                command,
                new Dictionary<string, object?> { ["@ID"] = 1 },
                ConvertParameterTypes(new Dictionary<string, SqlDbType> { ["@id"] = SqlDbType.Int }),
                new Dictionary<string, ParameterDirection> { ["@id"] = ParameterDirection.InputOutput });
            return (SqlParameter)command.Parameters[0];
        }
    }

    private sealed class MySqlParameterProbe : DBAClientX.MySql
    {
        public MySqlParameter CaptureMetadata(string? value)
        {
            using var command = new MySqlCommand();
            AddParameters(command,
                new Dictionary<string, object?> { ["@VALUE"] = new DbaParameterValue(value, DbType.AnsiString, 4000) },
                ConvertParameterTypes(new Dictionary<string, MySqlDbType> { ["@value"] = MySqlDbType.VarChar }),
                new Dictionary<string, ParameterDirection> { ["@value"] = ParameterDirection.InputOutput });
            return (MySqlParameter)command.Parameters[0];
        }

        public MySqlParameter Capture()
        {
            using var command = new MySqlCommand();
            AddParameters(
                command,
                new Dictionary<string, object?> { ["@ID"] = 1 },
                ConvertParameterTypes(new Dictionary<string, MySqlDbType> { ["@id"] = MySqlDbType.Int32 }),
                new Dictionary<string, ParameterDirection> { ["@id"] = ParameterDirection.InputOutput });
            return (MySqlParameter)command.Parameters[0];
        }
    }

    private sealed class PostgreSqlParameterProbe : DBAClientX.PostgreSql
    {
        public NpgsqlParameter CaptureMetadata(string? value)
        {
            using var command = new NpgsqlCommand();
            AddParameters(command,
                new Dictionary<string, object?> { ["@VALUE"] = new DbaParameterValue(value, DbType.AnsiString, 4000) },
                ConvertParameterTypes(new Dictionary<string, NpgsqlDbType> { ["@value"] = NpgsqlDbType.Text }),
                new Dictionary<string, ParameterDirection> { ["@value"] = ParameterDirection.InputOutput });
            return (NpgsqlParameter)command.Parameters[0];
        }

        public NpgsqlParameter Capture()
        {
            using var command = new NpgsqlCommand();
            AddParameters(
                command,
                new Dictionary<string, object?> { ["@ID"] = 1 },
                ConvertParameterTypes(new Dictionary<string, NpgsqlDbType> { ["@id"] = NpgsqlDbType.Integer }),
                new Dictionary<string, ParameterDirection> { ["@id"] = ParameterDirection.InputOutput });
            return (NpgsqlParameter)command.Parameters[0];
        }
    }

    private sealed class OracleParameterProbe : DBAClientX.Oracle
    {
        public OracleParameter CaptureMetadata(string? value)
        {
            using var command = new OracleCommand();
            AddParameters(command,
                new Dictionary<string, object?> { ["@VALUE"] = new DbaParameterValue(value, DbType.AnsiString, 4000) },
                ConvertParameterTypes(new Dictionary<string, OracleDbType> { ["@value"] = OracleDbType.NVarchar2 }),
                new Dictionary<string, ParameterDirection> { ["@value"] = ParameterDirection.InputOutput });
            return (OracleParameter)command.Parameters[0];
        }

        public OracleParameter Capture()
        {
            using var command = new OracleCommand();
            AddParameters(
                command,
                new Dictionary<string, object?> { [":ID"] = 1 },
                ConvertParameterTypes(new Dictionary<string, OracleDbType> { [":id"] = OracleDbType.Int32 }),
                new Dictionary<string, ParameterDirection> { [":id"] = ParameterDirection.InputOutput });
            return (OracleParameter)command.Parameters[0];
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("é漢字")]
    public void ProviderSpecificParameterMetadata_UnwrapsValueAndSizeWithoutReplacingNativeType(string? value)
    {
        var sql = new SqlServerParameterProbe().CaptureMetadata(value);
        var mysql = new MySqlParameterProbe().CaptureMetadata(value);
        var postgreSql = new PostgreSqlParameterProbe().CaptureMetadata(value);
        var oracle = new OracleParameterProbe().CaptureMetadata(value);
        Assert.Equal(SqlDbType.NVarChar, sql.SqlDbType);
        Assert.Equal(MySqlDbType.VarChar, mysql.MySqlDbType);
        Assert.Equal(NpgsqlDbType.Text, postgreSql.NpgsqlDbType);
        Assert.Equal(OracleDbType.NVarchar2, oracle.OracleDbType);
        foreach (System.Data.Common.DbParameter parameter in new System.Data.Common.DbParameter[] { sql, mysql, postgreSql, oracle })
        {
            Assert.Equal(value ?? (object)DBNull.Value, parameter.Value);
            Assert.Equal(4000, parameter.Size);
            Assert.Equal(ParameterDirection.InputOutput, parameter.Direction);
        }
    }
    [Fact]
    public void ProviderSpecificParameterTypes_MatchKeysCaseInsensitively()
    {
        var sql = new SqlServerParameterProbe().Capture();
        var mysql = new MySqlParameterProbe().Capture();
        var postgreSql = new PostgreSqlParameterProbe().Capture();
        var oracle = new OracleParameterProbe().Capture();

        Assert.Equal(SqlDbType.Int, sql.SqlDbType);
        Assert.Equal(ParameterDirection.InputOutput, sql.Direction);

        Assert.Equal(MySqlDbType.Int32, mysql.MySqlDbType);
        Assert.Equal(ParameterDirection.InputOutput, mysql.Direction);

        Assert.Equal(NpgsqlDbType.Integer, postgreSql.NpgsqlDbType);
        Assert.Equal(ParameterDirection.InputOutput, postgreSql.Direction);

        Assert.Equal(OracleDbType.Int32, oracle.OracleDbType);
        Assert.Equal(ParameterDirection.InputOutput, oracle.Direction);
    }
}
