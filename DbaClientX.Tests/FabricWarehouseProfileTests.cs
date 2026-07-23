using System.Data;
using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class FabricWarehouseProfileTests
{
    [Fact]
    public void BuildConnectionString_ProducesEncryptedTokenReadyTdsSettings()
    {
        var connectionString = FabricWarehouseProfile.BuildConnectionString(
            "warehouse-id.datawarehouse.fabric.microsoft.com",
            "Reporting",
            connectTimeoutSeconds: 30,
            applicationName: "DbaClientX.Tests");
        var builder = new SqlConnectionStringBuilder(connectionString);

        Assert.Equal("tcp:warehouse-id.datawarehouse.fabric.microsoft.com,1433", builder.DataSource);
        Assert.Equal("Reporting", builder.InitialCatalog);
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, builder.Encrypt);
        Assert.False(builder.TrustServerCertificate);
        Assert.False(builder.IntegratedSecurity);
        Assert.False(builder.MultipleActiveResultSets);
        Assert.True(builder.Pooling);
        Assert.Equal(30, builder.ConnectTimeout);
        Assert.Equal("DbaClientX.Tests", builder.ApplicationName);
        Assert.True(string.IsNullOrEmpty(builder.UserID));
        Assert.True(string.IsNullOrEmpty(builder.Password));
    }

    [Theory]
    [InlineData("Server=sql.contoso.com;Database=Reporting;Encrypt=True")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com,1444;Database=Reporting;Encrypt=True")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=master;Encrypt=True")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=False")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True;TrustServerCertificate=True")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True;MultipleActiveResultSets=True")]
    [InlineData("Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True;User ID=sql-user;Password=secret")]
    public void ValidateConnectionString_RejectsUnsupportedWarehouseSettings(string connectionString)
        => Assert.Throws<ArgumentException>(() => FabricWarehouseProfile.ValidateConnectionString(connectionString));

    [Fact]
    public void ValidateConnectionString_RejectsAuthenticationKeywordWithTokenCallback()
    {
        const string connectionString =
            "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True;Authentication=Active Directory Default";

        FabricWarehouseProfile.ValidateConnectionString(connectionString);
        Assert.Throws<ArgumentException>(() =>
            FabricWarehouseProfile.ValidateConnectionString(connectionString, usesAccessTokenCallback: true));
    }

    [Fact]
    public void CreateConnection_AppliesCallerOwnedFactoryAndReusableTokenCallback()
    {
        const string connectionString =
            "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True";
        var factoryCalls = 0;
        Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>> callback =
            static (_, _) => Task.FromResult(new SqlAuthenticationToken(
                "test-token",
                DateTimeOffset.UtcNow.AddHours(1)));
        var client = new ExposedSqlServer
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse,
                AccessTokenCallback = callback,
                ConnectionFactory = value =>
                {
                    factoryCalls++;
                    return new SqlConnection(value);
                }
            }
        };

        using var connection = client.CreateConnectionForTest(connectionString);

        Assert.Equal(1, factoryCalls);
        Assert.Same(callback, connection.AccessTokenCallback);
    }

    [Fact]
    public void CreateConnection_RejectsNullFromCallerOwnedFactory()
    {
        const string connectionString =
            "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True";
        var client = new ExposedSqlServer
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse,
                ConnectionFactory = static _ => null!
            }
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            client.CreateConnectionForTest(connectionString));

        Assert.Contains("factory returned null", exception.Message);
    }

    [Fact]
    public void BulkInsert_RejectsIgnoredFabricOptionsBeforeOpeningConnection()
    {
        const string connectionString =
            "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True";
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var client = new ExposedSqlServer
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse
            }
        };

        var exception = Assert.Throws<NotSupportedException>(() => client.BulkInsert(
            connectionString,
            table,
            "dbo.Rows",
            new SqlServerBulkInsertOptions
            {
                BulkCopyOptions = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls
            }));

        Assert.Contains("TableLock", exception.Message);
        Assert.Contains("KeepNulls", exception.Message);
        Assert.Equal(0, client.CreateConnectionCalls);
    }

    [Fact]
    public void GetIgnoredBulkCopyOptions_ReportsOnlyDocumentedIgnoredFlags()
    {
        var ignored = FabricWarehouseProfile.GetIgnoredBulkCopyOptions(
            SqlBulkCopyOptions.CheckConstraints |
            SqlBulkCopyOptions.TableLock |
            SqlBulkCopyOptions.KeepNulls |
            SqlBulkCopyOptions.FireTriggers |
            SqlBulkCopyOptions.KeepIdentity);

        Assert.Equal(
            new[]
            {
                SqlBulkCopyOptions.CheckConstraints,
                SqlBulkCopyOptions.TableLock,
                SqlBulkCopyOptions.KeepNulls,
                SqlBulkCopyOptions.FireTriggers
            },
            ignored);
        Assert.DoesNotContain(SqlBulkCopyOptions.KeepIdentity, ignored);
    }

    private sealed class ExposedSqlServer : SqlServer
    {
        public int CreateConnectionCalls { get; private set; }

        public SqlConnection CreateConnectionForTest(string connectionString)
            => base.CreateConnection(connectionString);

        protected override SqlConnection CreateConnection(string connectionString)
        {
            CreateConnectionCalls++;
            return base.CreateConnection(connectionString);
        }
    }
}
