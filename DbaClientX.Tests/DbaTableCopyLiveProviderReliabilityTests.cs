using System.Data;
using System.Data.Common;
using DBAClientX;
using DBAClientX.DataMovement;
using MySqlConnector;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyLiveProviderReliabilityTests
{
    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION", false)]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION", true)]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION", false)]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION", true)]
    [Trait("Category", "LiveProvider")]
    public async Task ClearDestination_PreflightsRelatedTablesInOneTransaction(
        DbaTableCopyProvider provider,
        string environmentVariable,
        bool verifyContent)
    {
        var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceParent = "dbax_sp_" + suffix;
        string sourceChild = "dbax_sc_" + suffix;
        string destinationParent = "dbax_dp_" + suffix;
        string destinationChild = "dbax_dc_" + suffix;
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        string Quote(string name) => provider == DbaTableCopyProvider.PostgreSql ? $"\"{name}\"" : $"`{name}`";
        string engine = provider == DbaTableCopyProvider.MySql ? " ENGINE=InnoDB" : string.Empty;
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(sourceParent)} (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(sourceChild)} (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(destinationParent)} (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL){engine}");
            await ExecuteAsync(connection, $"CREATE TABLE {Quote(destinationChild)} (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, CONSTRAINT {Quote("fk_" + suffix)} FOREIGN KEY (parent_id) REFERENCES {Quote(destinationParent)} (id)){engine}");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(sourceParent)} VALUES (1, 'first parent'), (2, 'new parent')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(sourceChild)} VALUES (20, 2, 'new child')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(destinationParent)} VALUES (1, 'old parent')");
            await ExecuteAsync(connection, $"INSERT INTO {Quote(destinationChild)} VALUES (10, 1, 'old child')");

            var source = CreateAdapter(provider, connectionString!, new[] { "id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definitions = new[]
            {
                new DbaTableCopyDefinition(sourceParent, destinationParent, new[] { "id" }) { UseKeysetPagination = true },
                new DbaTableCopyDefinition(sourceChild, destinationChild, new[] { "id" }) { UseKeysetPagination = true }
            };
            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                definitions,
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    VerifyContent = verifyContent,
                    PageSize = 1
                });

            Assert.Equal(3, result.CopiedRows);
            Assert.True(result.Verified);
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT COUNT(*) FROM {Quote(destinationParent)}")));
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT MAX(id) FROM {Quote(destinationParent)}")));
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT parent_id FROM {Quote(destinationChild)}")));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationChild));
            await TryExecuteAsync(connection, DropTableSql(provider, destinationParent));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceChild));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceParent));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointIdentity_DistinguishesCaseOnlyTablesOnCaseSensitiveServers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        int lowerCaseTableNames = Convert.ToInt32(await ExecuteScalarAsync(connection, "SELECT @@lower_case_table_names"));
        Assert.SkipWhen(lowerCaseTableNames != 0, "This contract requires a case-sensitive MySQL table-name server.");

        string lower = "dbaxcase_" + Guid.NewGuid().ToString("N").Substring(0, 12);
        string upper = lower.ToUpperInvariant();
        string suffix = Guid.NewGuid().ToString("N");
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{lower}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"CREATE TABLE `{upper}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            var adapter = new MySqlTableCopyAdapter(connectionString!);
            var lowerDefinition = new DbaTableCopyDefinition("unused", lower);
            var upperDefinition = new DbaTableCopyDefinition("unused", upper);
            var lowerCheckpoint = CreateInitialCheckpoint("case-lower-" + suffix);
            var upperCheckpoint = CreateInitialCheckpoint("case-upper-" + suffix);

            await adapter.InitializeCheckpointAsync(lowerDefinition, lowerCheckpoint, clearDestination: false);
            await adapter.InitializeCheckpointAsync(upperDefinition, upperCheckpoint, clearDestination: false);

            Assert.Equal(lowerCheckpoint, await adapter.ReadCheckpointAsync(lowerDefinition));
            Assert.Equal(upperCheckpoint, await adapter.ReadCheckpointAsync(upperDefinition));
        }
        finally
        {
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{upper}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{lower}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlProviderNativeValues_RejectCrossProviderCopyBeforeWriting()
    {
        var postgreSqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        var mySqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(postgreSqlConnectionString) || string.IsNullOrWhiteSpace(mySqlConnectionString),
            "Set both PostgreSQL and MySQL live-provider connection strings.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_native_source_" + suffix;
        string destinationTable = "dbax_native_destination_" + suffix;
        await using var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString!);
        await using var mySqlConnection = new MySqlConnection(mySqlConnectionString!);
        await postgreSqlConnection.OpenAsync();
        await mySqlConnection.OpenAsync();
        try
        {
            await ExecuteAsync(postgreSqlConnection, $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, address INET NOT NULL)");
            await ExecuteAsync(postgreSqlConnection, $"INSERT INTO \"{sourceTable}\" VALUES (1, '192.0.2.42')");
            await ExecuteAsync(mySqlConnection, $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, address TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(mySqlConnection, $"INSERT INTO `{destinationTable}` VALUES (99, 'preserved')");

            var source = new PostgreSqlTableCopyAdapter(postgreSqlConnectionString!, new[] { "id" });
            var destination = new MySqlTableCopyAdapter(mySqlConnectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("provider-specific type", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT COUNT(*) FROM `{destinationTable}`")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT id FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(mySqlConnection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(postgreSqlConnection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlArrays_RejectCrossProviderCopyBeforeWriting()
    {
        var postgreSqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        var mySqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(postgreSqlConnectionString) || string.IsNullOrWhiteSpace(mySqlConnectionString),
            "Set both PostgreSQL and MySQL live-provider connection strings.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_array_portability_source_" + suffix;
        string destinationTable = "dbax_array_portability_destination_" + suffix;
        await using var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString!);
        await using var mySqlConnection = new MySqlConnection(mySqlConnectionString!);
        await postgreSqlConnection.OpenAsync();
        await mySqlConnection.OpenAsync();
        try
        {
            await ExecuteAsync(postgreSqlConnection, $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, values INTEGER[] NOT NULL)");
            await ExecuteAsync(postgreSqlConnection, $"INSERT INTO \"{sourceTable}\" VALUES (1, ARRAY[1,2,3])");
            await ExecuteAsync(mySqlConnection, $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, values_json TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(mySqlConnection, $"INSERT INTO `{destinationTable}` VALUES (99, '[99]')");

            var source = new PostgreSqlTableCopyAdapter(postgreSqlConnectionString!, new[] { "id" });
            var destination = new MySqlTableCopyAdapter(mySqlConnectionString!);
            var definition = new DbaTableCopyDefinition(
                sourceTable,
                destinationTable,
                new[] { "id" },
                ColumnMappings: new Dictionary<string, string> { ["values"] = "values_json" },
                ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>
                {
                    ["values_json"] = DbaTableCopyColumnType.String
                });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("provider-specific type", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("not lossless", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT COUNT(*) FROM `{destinationTable}`")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT id FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(mySqlConnection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(postgreSqlConnection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlUnmappedEnums_RejectBeforeClearingRows(bool isArray)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string enumType = "dbax_enum_" + suffix;
        string sourceTable = "dbax_enum_source_" + suffix;
        string destinationTable = "dbax_enum_dest_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TYPE \"{enumType}\" AS ENUM ('ready', 'done')");
            string sourceType = $"\"{enumType}\"" + (isArray ? "[]" : string.Empty);
            string sourceValue = isArray
                ? $"ARRAY['ready'::\"{enumType}\"]"
                : $"'ready'::\"{enumType}\"";
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, state {sourceType} NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, {sourceValue})");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id BIGINT NOT NULL PRIMARY KEY, state TEXT NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var source = new PostgreSqlTableCopyAdapter(connectionString!, new[] { "id" });
            var destination = new PostgreSqlTableCopyAdapter(connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true }));

            Assert.Contains(isArray ? "enum array" : "an enum", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("explicit Npgsql enum mapping", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || state FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
            await TryExecuteAsync(connection, $"DROP TYPE IF EXISTS \"{enumType}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlInfinitySentinels_RejectCrossProviderCopyBeforeWriting()
    {
        string? postgreSqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        string? mySqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(postgreSqlConnectionString) || string.IsNullOrWhiteSpace(mySqlConnectionString),
            "Set both PostgreSQL and MySQL live-provider connection strings.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_infinity_source_" + suffix;
        string destinationTable = "dbax_infinity_destination_" + suffix;
        await using var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString!);
        await using var mySqlConnection = new MySqlConnection(mySqlConnectionString!);
        await postgreSqlConnection.OpenAsync();
        await mySqlConnection.OpenAsync();
        try
        {
            await ExecuteAsync(
                postgreSqlConnection,
                $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, event_date DATE NOT NULL, event_time TIMESTAMP NOT NULL)");
            await ExecuteAsync(postgreSqlConnection, $"INSERT INTO \"{sourceTable}\" VALUES (1, 'infinity', '-infinity')");
            await ExecuteAsync(
                mySqlConnection,
                $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, event_date DATE NOT NULL, event_time DATETIME(6) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(mySqlConnection, $"INSERT INTO `{destinationTable}` VALUES (99, '2026-01-01', '2026-01-01 00:00:00')");

            var source = new PostgreSqlTableCopyAdapter(postgreSqlConnectionString!, new[] { "id" });
            var destination = new MySqlTableCopyAdapter(mySqlConnectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("infinity sentinels", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT COUNT(*) FROM `{destinationTable}`")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(mySqlConnection, $"SELECT id FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(mySqlConnection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(postgreSqlConnection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlNumericArrayNaN_RejectsCopyBeforeWritingAnyPage()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_nan_source_" + suffix;
        string destinationTable = "dbax_nan_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, amounts NUMERIC(10,2)[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, ARRAY[1.25::NUMERIC(10,2)]), (2, ARRAY['NaN'::NUMERIC])");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id BIGINT NOT NULL PRIMARY KEY, amounts NUMERIC(10,2)[] NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, ARRAY[99.00::NUMERIC(10,2)])");

            var source = new PostgreSqlTableCopyAdapter(connectionString!, new[] { "id" });
            var destination = new PostgreSqlTableCopyAdapter(connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { PageSize = 1 }));

            Assert.Contains("numeric NaN", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT COUNT(*) FROM \"{destinationTable}\"")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT id FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlOversizedNumericArray_RejectsCopyBeforeWritingAnyPage()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_numeric_array_source_" + suffix;
        string destinationTable = "dbax_numeric_array_dest_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, amounts NUMERIC(100,0)[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, ARRAY[1::NUMERIC(100,0)]), (2, ARRAY[1234567890123456789012345678901234567890::NUMERIC(100,0)])");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id BIGINT NOT NULL PRIMARY KEY, amounts NUMERIC(100,0)[] NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, ARRAY[99::NUMERIC(100,0)])");

            var source = new PostgreSqlTableCopyAdapter(connectionString!, new[] { "id" });
            var destination = new PostgreSqlTableCopyAdapter(connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { PageSize = 1 }));

            Assert.Contains("System.Decimal precision", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT COUNT(*) FROM \"{destinationTable}\"")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT id FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlExcludedOversizedNumeric_IsNotMaterialized()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_excluded_numeric_" + suffix;
        string sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-excluded-numeric-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id BIGINT NOT NULL PRIMARY KEY, amount NUMERIC(100,0) NOT NULL, payload TEXT NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, 1234567890123456789012345678901234567890, 'copied')");
            using (var sqlite = new SQLite())
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE DestinationRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");

            var source = new PostgreSqlTableCopyAdapter(connectionString!, new[] { "id" });
            var destination = new SQLiteTableCopyAdapter(sqlitePath);
            var definition = new DbaTableCopyDefinition(
                sourceTable,
                "DestinationRows",
                new[] { "id" },
                ExcludedColumns: new HashSet<string>(StringComparer.Ordinal) { "amount" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1 });

            Assert.Equal(1, result.CopiedRows);
            using var verification = new SQLite();
            Assert.Equal("1:copied", Convert.ToString(verification.ExecuteScalar(sqlitePath, "SELECT id || ':' || payload FROM DestinationRows")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
            if (File.Exists(sqlitePath)) File.Delete(sqlitePath);
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlBulkCopy_WritesProviderNeutralYearMonthIntervalsLosslessly()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_interval_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{table}\" (id BIGINT NOT NULL PRIMARY KEY, period INTERVAL NOT NULL)");
            using var page = new DataTable(table);
            page.Columns.Add("id", typeof(long));
            page.Columns.Add("period", typeof(DbaYearMonthInterval));
            page.Rows.Add(1L, new DbaYearMonthInterval(-27));

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", table, new[] { "id" });
            await destination.WritePageAsync(definition, page, new DbaTableCopyOptions());

            Assert.Equal(-27L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT EXTRACT(YEAR FROM period)::bigint * 12 + EXTRACT(MONTH FROM period)::bigint FROM \"{table}\" WHERE id = 1")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_PreservesCalendarIntervalComponents()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_interval_source_" + suffix;
        string destinationTable = "dbax_interval_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, period interval NOT NULL)");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, period interval NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, INTERVAL '1 year 2 mons 3 days 04:05:06.123456')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(14L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT EXTRACT(YEAR FROM period)::bigint * 12 + EXTRACT(MONTH FROM period)::bigint FROM \"{destinationTable}\"")));
            Assert.Equal(3L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT EXTRACT(DAY FROM period)::bigint FROM \"{destinationTable}\"")));
            Assert.Equal(14_706_123_456L, Convert.ToInt64(await ExecuteScalarAsync(connection, $"SELECT (EXTRACT(HOUR FROM period) * 3600000000 + EXTRACT(MINUTE FROM period) * 60000000 + EXTRACT(SECOND FROM period) * 1000000)::bigint FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlConsistentRead_RejectsViewsDependingOnForeignTables()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string remoteTable = "dbax_view_remote_" + suffix;
        string foreignTable = "dbax_view_foreign_" + suffix;
        string view = "dbax_foreign_view_" + suffix;
        string server = "dbax_view_server_" + suffix;
        var builder = new NpgsqlConnectionStringBuilder(connectionString!);
        string remoteUsername = builder.Username
            ?? throw new InvalidOperationException("The PostgreSQL live-provider connection requires a username.");
        string remotePassword = builder.Password
            ?? throw new InvalidOperationException("The PostgreSQL live-provider connection requires a password.");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS postgres_fdw");
            await ExecuteAsync(connection, $"CREATE TABLE \"{remoteTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(connection, $"CREATE SERVER \"{server}\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '{builder.Port}', dbname '{EscapeSqlLiteral(connection.Database)}')");
            await ExecuteAsync(connection, $"CREATE USER MAPPING FOR CURRENT_USER SERVER \"{server}\" OPTIONS (user '{EscapeSqlLiteral(remoteUsername)}', password '{EscapeSqlLiteral(remotePassword)}')");
            await ExecuteAsync(connection, $"CREATE FOREIGN TABLE \"{foreignTable}\" (id bigint NOT NULL, payload text NOT NULL) SERVER \"{server}\" OPTIONS (schema_name 'public', table_name '{remoteTable}')");
            await ExecuteAsync(connection, $"CREATE VIEW \"{view}\" AS SELECT id, payload FROM \"{foreignTable}\"");

            var source = CreateAdapter(
                DbaTableCopyProvider.PostgreSql,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            var definition = new DbaTableCopyDefinition(view, view, new[] { "id" }) { UseKeysetPagination = true };

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                ((IDbaTableCopyDefinitionReadSession)source).OpenReadSessionAsync(new[] { definition }));

            Assert.Contains("view dependency graph", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP VIEW IF EXISTS \"{view}\"");
            await TryExecuteAsync(connection, $"DROP FOREIGN TABLE IF EXISTS \"{foreignTable}\"");
            await TryExecuteAsync(connection, $"DROP SERVER IF EXISTS \"{server}\" CASCADE");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{remoteTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_RejectsNontransactionalDestinationBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_myisam_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=MyISAM");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (1, 'preserve')");

            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "myisam-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("InnoDB", exception.Message, StringComparison.Ordinal);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointStorage_RejectsMissingSelectedDatabaseBeforeChangingQualifiedDestination()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var configured = new MySqlConnectionStringBuilder(connectionString!);
        Assert.False(string.IsNullOrWhiteSpace(configured.Database));
        string database = configured.Database;
        var table = "dbax_no_database_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{table}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{table}` VALUES (1, 'preserve')");

            configured.Database = string.Empty;
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, configured.ConnectionString);
            var definition = new DbaTableCopyDefinition("unused", $"`{database}`.`{table}`", new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "no-database-" + table,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("selected database", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{table}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{table}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlConsistentRead_RejectsNontransactionalSourceBeforeCopyingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_myisam_source_" + suffix;
        var destinationTable = "dbax_innodb_destination_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=MyISAM");
            await ExecuteAsync(connection, $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{sourceTable}` VALUES (1, 'source')");

            var source = CreateAdapter(
                DbaTableCopyProvider.MySql,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("InnoDB", exception.Message, StringComparison.Ordinal);
            Assert.Equal(0L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlConsistentRead_ToleratesMissingSourceAsEmpty()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var source = new MySqlTableCopyAdapter(
            connectionString!,
            new[] { "id" },
            treatMissingTablesAsEmpty: true)
        {
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot
        };
        var definition = new DbaTableCopyDefinition(
            "dbax_missing_" + Guid.NewGuid().ToString("N"),
            "unused",
            new[] { "id" })
        {
            UseKeysetPagination = true
        };

        using var session = await source.OpenReadSessionAsync(new[] { definition });
        using var page = await source.ReadPageAsync(new(definition, null, 1));

        Assert.Empty(page.Data.Rows.Cast<DataRow>());
        Assert.Null(page.ContinuationToken);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlConsistentRead_ToleratesMissingSourceWithoutAbortingTransaction()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var source = new PostgreSqlTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.PostgreSql,
            ConnectionString = connectionString!,
            DefaultOrderByColumns = new[] { "id" },
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot,
            TreatMissingTablesAsEmpty = true
        });
        var definition = new DbaTableCopyDefinition(
            "dbax_missing_" + Guid.NewGuid().ToString("N"),
            "unused",
            new[] { "id" })
        {
            UseKeysetPagination = true
        };

        using var session = await source.OpenReadSessionAsync(new[] { definition });
        using var page = await source.ReadPageAsync(new(definition, null, 1));

        Assert.Empty(page.Data.Rows.Cast<DataRow>());
        Assert.Null(page.ContinuationToken);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_UsesMappedNamesWhenPhysicalColumnOrderDiffers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_mapping_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-mapping-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (destination_payload VARCHAR(64) NOT NULL, destination_id BIGINT NOT NULL PRIMARY KEY)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (SourceId INTEGER NOT NULL PRIMARY KEY, SourcePayload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'One'), (2, 'Dwa')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "SourceId" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                destinationTable,
                new[] { "SourceId" },
                ColumnMappings: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["SourceId"] = "destination_id",
                    ["SourcePayload"] = "destination_payload"
                })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    CheckpointId = "mapping-" + suffix,
                    PageSize = 1,
                    VerifyContent = true
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(
                "1:One",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(destination_id, ':', destination_payload) FROM `{destinationTable}` ORDER BY destination_id LIMIT 1")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_PreflightsRequiredDestinationColumnsBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_schema_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-schema-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, required_value INT NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (99, 'preserve', 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "schema-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("required_value", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', payload, ':', required_value) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_PreflightsRequiredDestinationColumnsBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_schema_" + suffix;
        var firstSearchPathSchema = "dbax_empty_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-schema-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA \"{firstSearchPathSchema}\"");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE public.\"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL, required_value integer NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO public.\"{destinationTable}\" VALUES (99, 'preserve', 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destinationConnection = new NpgsqlConnectionStringBuilder(connectionString!)
            {
                SearchPath = $"{firstSearchPathSchema},public"
            }.ConnectionString;
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, destinationConnection);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "schema-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("required_value", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload || ':' || required_value FROM public.\"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS public.\"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP SCHEMA IF EXISTS \"{firstSearchPathSchema}\" CASCADE");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_PreflightsIncompatibleTypesBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_types_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-types-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, required_value integer NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, required_value TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'not-an-integer')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "types-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("CLR types", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || required_value FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCheckpointedCopy_PreflightsIncompatibleTypesBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_types_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-mysql-types-" + suffix + ".sqlite");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL PRIMARY KEY, required_value integer NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, required_value TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'not-an-integer')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "types-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', required_value) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.MySql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Theory]
    [Trait("Category", "LiveProvider")]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    public async Task CheckpointedCopy_PreflightsActualDestinationConstraintsBeforeClearingRows(
        DbaTableCopyProvider provider)
    {
        string environmentVariable = provider == DbaTableCopyProvider.PostgreSql
            ? "DBACLIENTX_POSTGRESQL_TEST_CONNECTION"
            : "DBACLIENTX_MYSQL_TEST_CONNECTION";
        string? connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string destinationTable = "dbax_constraints_" + suffix;
        string sqlitePath = Path.Combine(Path.GetTempPath(), $"dbax-{provider}-constraints-{suffix}.sqlite");
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            string createSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, constrained_value bigint NOT NULL CHECK (constrained_value > 0))"
                : $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL PRIMARY KEY, constrained_value bigint NOT NULL CHECK (constrained_value > 0)) ENGINE=InnoDB";
            await ExecuteAsync(connection, createSql);
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)"
                    : $"INSERT INTO `{destinationTable}` VALUES (99, 7)");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sqlitePath,
                    "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, constrained_value INTEGER NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 1), (2, -1)");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "constraints-" + suffix,
                        ClearDestination = true,
                        PageSize = 2
                    }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            string preservedValueSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT id || ':' || constrained_value FROM \"{destinationTable}\""
                : $"SELECT CONCAT(id, ':', constrained_value) FROM `{destinationTable}`";
            Assert.Equal("99:7", Convert.ToString(await ExecuteScalarAsync(connection, preservedValueSql)));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DeleteCheckpointSql(provider, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointStorage_RejectsUnloggedTableBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string schema = "dbax_checkpoint_" + suffix;
        string destinationTable = "destination_rows";
        var builder = new NpgsqlConnectionStringBuilder(connectionString!) { SearchPath = schema };
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA \"{schema}\"");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{schema}\".\"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{schema}\".\"{destinationTable}\" VALUES (99, 'preserve')");
            await ExecuteAsync(
                connection,
                $"CREATE UNLOGGED TABLE \"{schema}\".\"DbaClientX_TableCopyCheckpoints\" (id integer)");

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, builder.ConnectionString);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "checkpoint-storage-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("permanent logged table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{schema}\".\"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlClearDestination_RejectsTriggersOnLeafPartitionsBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string destinationTable = "dbax_trigger_parent_" + suffix;
        string partitionTable = "dbax_trigger_leaf_" + suffix;
        string triggerFunction = "dbax_trigger_fn_" + suffix;
        string sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-trigger-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL, payload text NOT NULL) PARTITION BY RANGE (id)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{partitionTable}\" PARTITION OF \"{destinationTable}\" FOR VALUES FROM (0) TO (1000)");
            await ExecuteAsync(
                connection,
                $"CREATE FUNCTION \"{triggerFunction}\"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$");
            await ExecuteAsync(
                connection,
                $"CREATE TRIGGER dbax_leaf_trigger BEFORE INSERT OR DELETE ON \"{partitionTable}\" FOR EACH ROW EXECUTE FUNCTION \"{triggerFunction}\"()");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "trigger-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("enabled INSERT or DELETE trigger", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{partitionTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\" CASCADE");
            await TryExecuteAsync(connection, $"DROP FUNCTION IF EXISTS \"{triggerFunction}\"() CASCADE");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_RejectsPartitionTreesWithForeignLeafTables()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string destinationTable = "dbax_foreign_parent_" + suffix;
        string localPartition = "dbax_foreign_local_" + suffix;
        string foreignPartition = "dbax_foreign_leaf_" + suffix;
        string foreignServer = "dbax_foreign_server_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS postgres_fdw");
            await ExecuteAsync(
                connection,
                $"CREATE SERVER \"{foreignServer}\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', dbname '{connection.Database}')");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL, payload text NOT NULL) PARTITION BY RANGE (id)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{localPartition}\" PARTITION OF \"{destinationTable}\" FOR VALUES FROM (0) TO (100)");
            await ExecuteAsync(
                connection,
                $"CREATE FOREIGN TABLE \"{foreignPartition}\" (id bigint NOT NULL, payload text NOT NULL, CHECK (id >= 100 AND id < 200)) SERVER \"{foreignServer}\" OPTIONS (schema_name 'public', table_name 'remote_rows')");
            await ExecuteAsync(
                connection,
                $"ALTER TABLE \"{destinationTable}\" ATTACH PARTITION \"{foreignPartition}\" FOR VALUES FROM (100) TO (200)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "foreign-tree-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("cannot be resolved to a PostgreSQL table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{localPartition}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\" CASCADE");
            await TryExecuteAsync(connection, $"DROP FOREIGN TABLE IF EXISTS \"{foreignPartition}\" CASCADE");
            await TryExecuteAsync(connection, $"DROP SERVER IF EXISTS \"{foreignServer}\" CASCADE");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_RejectsViewBeforeClearingUnderlyingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var baseTable = "dbax_view_base_" + suffix;
        var destinationView = "dbax_view_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-pg-view-" + suffix + ".sqlite");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{baseTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{baseTable}\" VALUES (99, 'preserve')");
            await ExecuteAsync(
                connection,
                $"CREATE VIEW \"{destinationView}\" AS SELECT id, payload FROM \"{baseTable}\"");
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'new')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("SourceRows", destinationView, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions
                    {
                        CheckpointId = "view-" + suffix,
                        ClearDestination = true,
                        PageSize = 1
                    }));

            Assert.Contains("cannot be resolved to a PostgreSQL table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{baseTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP VIEW IF EXISTS \"{destinationView}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{baseTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCheckpointedCopy_RejectsUnloggedDestinationBeforeClearingRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_unlogged_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE UNLOGGED TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserve')");

            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition("unused", destinationTable, new[] { "id" });
            var checkpoint = new DbaTableCopyCheckpoint
            {
                CopyId = "unlogged-" + suffix,
                DefinitionFingerprint = new string('1', 64),
                SourceRows = 1,
                SourceContentHash = new string('2', 64),
                CopiedRows = 0,
                CopiedContentHash = new string('3', 64),
                Completed = false
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true));

            Assert.Contains("cannot be resolved to a PostgreSQL table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserve",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || payload FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, DeleteCheckpointSql(DbaTableCopyProvider.PostgreSql, suffix));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlKeysetRead_RoundTripsUnsignedBigIntBeyondInt64Range()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        var table = "dbax_unsigned_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{table}` (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO `{table}` VALUES (9223372036854775808, 'first'), (18446744073709551615, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "id" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));

            Assert.Equal(9223372036854775808UL, Assert.IsType<ulong>(first.Data.Rows[0]["id"]));
            Assert.Equal(ulong.MaxValue, Assert.IsType<ulong>(second.Data.Rows[0]["id"]));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{table}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlUnsignedBigInteger_RejectsCrossProviderCopyBeforeWriting()
    {
        string? mySqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        string? postgreSqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(mySqlConnectionString) || string.IsNullOrWhiteSpace(postgreSqlConnectionString),
            "Set both MySQL and PostgreSQL live-provider connection strings.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_unsigned_source_" + suffix;
        string destinationTable = "dbax_unsigned_destination_" + suffix;
        await using var mySqlConnection = new MySqlConnection(mySqlConnectionString!);
        await using var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString!);
        await mySqlConnection.OpenAsync();
        await postgreSqlConnection.OpenAsync();
        try
        {
            await ExecuteAsync(
                mySqlConnection,
                $"CREATE TABLE `{sourceTable}` (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(mySqlConnection, $"INSERT INTO `{sourceTable}` VALUES (18446744073709551615, 'source')");
            await ExecuteAsync(
                postgreSqlConnection,
                $"CREATE TABLE \"{destinationTable}\" (id NUMERIC(20,0) NOT NULL PRIMARY KEY, payload TEXT NOT NULL)");
            await ExecuteAsync(postgreSqlConnection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var source = new MySqlTableCopyAdapter(mySqlConnectionString!, new[] { "id" });
            var destination = new PostgreSqlTableCopyAdapter(postgreSqlConnectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" });

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("BIGINT UNSIGNED", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(postgreSqlConnection, $"SELECT COUNT(*) FROM \"{destinationTable}\"")));
            Assert.Equal(99L, Convert.ToInt64(await ExecuteScalarAsync(postgreSqlConnection, $"SELECT id FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(postgreSqlConnection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(mySqlConnection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlWideBit_RejectsCrossProviderCopyBeforeWriting()
    {
        string? mySqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        string? postgreSqlConnectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(mySqlConnectionString) || string.IsNullOrWhiteSpace(postgreSqlConnectionString),
            "Set both MySQL and PostgreSQL live-provider connection strings.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_bit_source_" + suffix;
        string destinationTable = "dbax_bit_destination_" + suffix;
        await using var mySqlConnection = new MySqlConnection(mySqlConnectionString!);
        await using var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString!);
        await mySqlConnection.OpenAsync();
        await postgreSqlConnection.OpenAsync();
        try
        {
            await ExecuteAsync(
                mySqlConnection,
                $"CREATE TABLE `{sourceTable}` (flags BIT(64) NOT NULL, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(mySqlConnection, $"INSERT INTO `{sourceTable}` VALUES (0xFFFFFFFFFFFFFFFF, 'source')");
            await ExecuteAsync(
                postgreSqlConnection,
                $"CREATE TABLE \"{destinationTable}\" (flags NUMERIC(20,0) NOT NULL, payload TEXT NOT NULL)");
            await ExecuteAsync(postgreSqlConnection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var source = new MySqlTableCopyAdapter(mySqlConnectionString!);
            var destination = new PostgreSqlTableCopyAdapter(postgreSqlConnectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable);

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }));

            Assert.Contains("BIT(64)", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    postgreSqlConnection,
                    $"SELECT flags || ':' || payload FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(postgreSqlConnection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(mySqlConnection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlVerifiedCopy_RoundTripsArbitraryPrecisionDecimalKeysets()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_decimal_source_" + suffix;
        string destinationTable = "dbax_decimal_destination_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"CREATE TABLE `{destinationTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY, payload VARCHAR(32) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(
                connection,
                $"INSERT INTO `{sourceTable}` VALUES (1000000000000000000000000000000, 'first'), (10000000000000000000000000000000, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "amount" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "amount" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    VerifyContent = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(
                "1000000000000000000000000000000,10000000000000000000000000000000",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT GROUP_CONCAT(CAST(amount AS CHAR) ORDER BY amount SEPARATOR ',') FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlArbitraryPrecisionDecimal_RejectsUnsupportedDestinationBeforeCopying()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL database.");

        string sourceTable = "dbax_decimal_portability_" + Guid.NewGuid().ToString("N");
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE `{sourceTable}` (amount DECIMAL(65,0) NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            var source = new MySqlTableCopyAdapter(connectionString!, new[] { "amount" });
            var definition = new DbaTableCopyDefinition(sourceTable, "Destination", new[] { "amount" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                source.ValidateDestinationCompatibilityAsync(
                    DbaTableCopyProvider.SQLite,
                    new[] { definition },
                    CancellationToken.None));

            Assert.Contains("28-digit numeric range supported by SQLite", exception.Message, StringComparison.Ordinal);
            Assert.Contains("convert it explicitly to String", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCopy_RejectsOversizedNumericShapeBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_numeric_source_" + suffix;
        string destinationTable = "dbax_numeric_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, amount numeric(65,0) NOT NULL)");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, amount numeric(65,0) NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, 1), (2, 1000000000000000000000000000000)");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 7)");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("System.Decimal precision", exception.Message, StringComparison.Ordinal);
            Assert.Equal(
                "99:7",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || amount FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlCopy_PreflightsCrossPageConstraintsBeforeClearingRows()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_constraint_source_" + suffix;
        string destinationTable = "dbax_constraint_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, code text NOT NULL)");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, code text NOT NULL UNIQUE)");
            await ExecuteAsync(connection, $"INSERT INTO \"{sourceTable}\" VALUES (1, 'duplicate'), (2, 'duplicate')");
            await ExecuteAsync(connection, $"INSERT INTO \"{destinationTable}\" VALUES (99, 'preserved')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("schema preflight", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT id || ':' || code FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [Trait("Category", "LiveProvider")]
    public async Task ClearDestination_RejectsOmittedGeneratorsWithoutAdvancingThem(DbaTableCopyProvider provider)
    {
        string environmentVariable = provider == DbaTableCopyProvider.PostgreSql
            ? "DBACLIENTX_POSTGRESQL_TEST_CONNECTION"
            : "DBACLIENTX_MYSQL_TEST_CONNECTION";
        string? connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N");
        string sourceTable = "dbax_gs_" + suffix;
        string destinationTable = "dbax_gd_" + suffix;
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            string createSource = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{sourceTable}\" (payload text NOT NULL PRIMARY KEY)"
                : $"CREATE TABLE `{sourceTable}` (payload varchar(50) NOT NULL PRIMARY KEY) ENGINE=InnoDB";
            string createDestination = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{destinationTable}\" (id bigserial NOT NULL PRIMARY KEY, payload text NOT NULL)"
                : $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB";
            await ExecuteAsync(connection, createSource);
            await ExecuteAsync(connection, createDestination);
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{sourceTable}\" VALUES ('new')"
                    : $"INSERT INTO `{sourceTable}` VALUES ('new')");
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{destinationTable}\" (id, payload) VALUES (99, 'preserved')"
                    : $"INSERT INTO `{destinationTable}` (id, payload) VALUES (99, 'preserved')");

            string generatorStateSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT last_value::text || ':' || is_called::text FROM \"{destinationTable}_id_seq\""
                : $"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{destinationTable}'";
            string? before = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            var source = CreateAdapter(provider, connectionString!, new[] { "payload" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "payload" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(before, Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql)));
            string preservedSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT id || ':' || payload FROM \"{destinationTable}\""
                : $"SELECT CONCAT(id, ':', payload) FROM `{destinationTable}`";
            Assert.Equal("99:preserved", Convert.ToString(await ExecuteScalarAsync(connection, preservedSql)));
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceTable));
        }
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [Trait("Category", "LiveProvider")]
    public async Task VerifiedCopy_DoesNotPreflightWriteRollbackUnsafeGenerators(DbaTableCopyProvider provider)
    {
        string environmentVariable = provider == DbaTableCopyProvider.PostgreSql
            ? "DBACLIENTX_POSTGRESQL_TEST_CONNECTION"
            : "DBACLIENTX_MYSQL_TEST_CONNECTION";
        string? connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_vgs_" + suffix;
        string destinationTable = "dbax_vgd_" + suffix;
        await using DbConnection connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            string createSource = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{sourceTable}\" (payload text NOT NULL PRIMARY KEY)"
                : $"CREATE TABLE `{sourceTable}` (payload varchar(50) NOT NULL PRIMARY KEY) ENGINE=InnoDB";
            string createDestination = provider == DbaTableCopyProvider.PostgreSql
                ? $"CREATE TABLE \"{destinationTable}\" (id bigserial NOT NULL PRIMARY KEY, payload text NOT NULL)"
                : $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB";
            await ExecuteAsync(connection, createSource);
            await ExecuteAsync(connection, createDestination);
            await ExecuteAsync(
                connection,
                provider == DbaTableCopyProvider.PostgreSql
                    ? $"INSERT INTO \"{sourceTable}\" VALUES ('new')"
                    : $"INSERT INTO `{sourceTable}` VALUES ('new')");

            string generatorStateSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT last_value::text || ':' || is_called::text FROM \"{destinationTable}_id_seq\""
                : $"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{destinationTable}'";
            string? before = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            var source = CreateAdapter(provider, connectionString!, new[] { "payload" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "payload" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { VerifyContent = true, PageSize = 1 });

            Assert.True(result.Verified);
            Assert.Equal(1, result.CopiedRows);
            string copiedSql = provider == DbaTableCopyProvider.PostgreSql
                ? $"SELECT id || ':' || payload FROM \"{destinationTable}\""
                : $"SELECT CONCAT(id, ':', payload) FROM `{destinationTable}`";
            Assert.Equal("1:new", Convert.ToString(await ExecuteScalarAsync(connection, copiedSql)));
            string? after = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            Assert.NotEqual(before, after);
            Assert.Equal(
                provider == DbaTableCopyProvider.PostgreSql ? "1:true" : "2",
                after?.ToLowerInvariant());
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DropTableSql(provider, sourceTable));
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlClearDestination_RejectsLaterNullAutoIncrementValuesWithoutAdvancingGenerator()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_autonull_s_" + suffix;
        string destinationTable = "dbax_autonull_d_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{sourceTable}` (id bigint NULL, payload varchar(50) NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{sourceTable}` VALUES (1, 'a-safe'), (NULL, 'b-new')");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` (id, payload) VALUES (99, 'preserved')");

            string generatorStateSql =
                $"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{destinationTable}'";
            string? before = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "payload" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "payload" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("auto-increment advances are not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(before, Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql)));
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', payload) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlClearDestination_RejectsLaterExplicitAutoIncrementAdvance()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated provider database.");

        string suffix = Guid.NewGuid().ToString("N")[..12];
        string sourceTable = "dbax_autonext_s_" + suffix;
        string destinationTable = "dbax_autonext_d_" + suffix;
        await using var connection = new MySqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{sourceTable}` (id bigint NOT NULL PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE `{destinationTable}` (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, payload varchar(50) NOT NULL) ENGINE=InnoDB");
            await ExecuteAsync(connection, $"INSERT INTO `{sourceTable}` VALUES (1, 'a-safe'), (150, 'b-advances')");
            await ExecuteAsync(connection, $"INSERT INTO `{destinationTable}` (id, payload) VALUES (99, 'preserved')");

            string generatorStateSql =
                $"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{destinationTable}'";
            string? before = Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql));
            var source = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.MySql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(
                    source,
                    destination,
                    new[] { definition },
                    new DbaTableCopyOptions { ClearDestination = true, PageSize = 1 }));

            Assert.Contains("current next value", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("auto-increment advances are not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(before, Convert.ToString(await ExecuteScalarAsync(connection, generatorStateSql)));
            Assert.Equal(
                "99:preserved",
                Convert.ToString(await ExecuteScalarAsync(
                    connection,
                    $"SELECT CONCAT(id, ':', payload) FROM `{destinationTable}`")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{destinationTable}`");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS `{sourceTable}`");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlKeysetRead_RoundTripsDateAndTimeKeys()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_temporal_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{table}\" (event_date date NOT NULL, event_time time without time zone NOT NULL, payload text NOT NULL, PRIMARY KEY (event_date, event_time))");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{table}\" VALUES (DATE '2026-09-20', TIME '01:02:03.000001', 'first'), (DATE '2026-09-20', TIME '01:02:03.000002', 'second'), (DATE '2026-09-21', TIME '00:00:00', 'third')");

            var source = CreateAdapter(
                DbaTableCopyProvider.PostgreSql,
                connectionString!,
                new[] { "event_date", "event_time" });
            var definition = new DbaTableCopyDefinition(
                table,
                table,
                new[] { "event_date", "event_time" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));
            using var third = await source.ReadPageAsync(new(definition, second.ContinuationToken, 1));

            Assert.IsType<DateOnly>(first.Data.Rows[0]["event_date"]);
            Assert.IsType<TimeOnly>(first.Data.Rows[0]["event_time"]);
            Assert.Equal("first", first.Data.Rows[0].Field<string>("payload"));
            Assert.Equal("second", second.Data.Rows[0].Field<string>("payload"));
            Assert.Equal("third", third.Data.Rows[0].Field<string>("payload"));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_NormalizesNetworkValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_network_source_" + suffix;
        var destinationTable = "dbax_network_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, address inet NOT NULL, subnet cidr NOT NULL, mac macaddr NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, address inet NOT NULL, subnet cidr NOT NULL, mac macaddr NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '192.0.2.42', '198.51.100.0/24', '00:11:22:aa:bb:cc'), (2, '2001:db8::42', '2001:db8::/48', '00:11:22:aa:bb:dd')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarAsync(
                connection,
                $"SELECT COUNT(*) FROM \"{destinationTable}\"")));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesArrayValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_array_source_" + suffix;
        var destinationTable = "dbax_array_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, ARRAY[1,2,3]), (2, ARRAY[4,5,6])");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesRangeAndMultirangeValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_range_source_" + suffix;
        var destinationTable = "dbax_range_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{sourceTable}\" (id bigint NOT NULL PRIMARY KEY, span int4range NOT NULL, spans int4multirange NOT NULL)");
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{destinationTable}\" (id bigint NOT NULL PRIMARY KEY, span int4range NOT NULL, spans int4multirange NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '[1,5)'::int4range, '{{[1,5),[10,20)}}'::int4multirange), (2, 'empty'::int4range, '{{[30,40)}}'::int4multirange)");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlVerifiedCopy_HashesGeometricValues()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var suffix = Guid.NewGuid().ToString("N");
        var sourceTable = "dbax_geometry_source_" + suffix;
        var destinationTable = "dbax_geometry_destination_" + suffix;
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            const string columns = "id bigint NOT NULL PRIMARY KEY, p point NOT NULL, l line NOT NULL, s lseg NOT NULL, b box NOT NULL, path path NOT NULL, polygon polygon NOT NULL, circle circle NOT NULL";
            await ExecuteAsync(connection, $"CREATE TABLE \"{sourceTable}\" ({columns})");
            await ExecuteAsync(connection, $"CREATE TABLE \"{destinationTable}\" ({columns})");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{sourceTable}\" VALUES (1, '(1,2)', '{{1,2,3}}', '[(1,2),(3,4)]', '(3,4),(1,2)', '[(1,2),(3,4)]', '((1,2),(3,4),(5,6))', '<(1,2),5>')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var destination = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!);
            var definition = new DbaTableCopyDefinition(sourceTable, destinationTable, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions { PageSize = 1, VerifyContent = true });

            Assert.True(result.Verified);
            Assert.Equal(1, result.CopiedRows);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{destinationTable}\"");
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{sourceTable}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlKeysetRead_RoundTripsDelimitedMixedCaseKey()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_delimited_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(
                connection,
                $"CREATE TABLE \"{table}\" (\"UserId\" bigint NOT NULL PRIMARY KEY, payload text NOT NULL)");
            await ExecuteAsync(
                connection,
                $"INSERT INTO \"{table}\" VALUES (1, 'first'), (2, 'second')");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "\"UserId\"" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "\"UserId\"" })
            {
                UseKeysetPagination = true
            };

            using var first = await source.ReadPageAsync(new(definition, null, 1));
            using var second = await source.ReadPageAsync(new(definition, first.ContinuationToken, 1));

            Assert.Equal(1L, first.Data.Rows[0].Field<long>("UserId"));
            Assert.Equal(2L, second.Data.Rows[0].Field<long>("UserId"));
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlBoundedRead_RejectsVariableSizeNativeArraysBeforeMaterializing()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");

        var table = "dbax_array_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE \"{table}\" (id bigint NOT NULL PRIMARY KEY, values integer[] NOT NULL)");
            await ExecuteAsync(connection, $"INSERT INTO \"{table}\" VALUES (1, ARRAY(SELECT generate_series(1, 10000)))");

            var source = CreateAdapter(DbaTableCopyProvider.PostgreSql, connectionString!, new[] { "id" });
            var definition = new DbaTableCopyDefinition(table, table, new[] { "id" })
            {
                UseKeysetPagination = true
            };

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
                source.ReadPageAsync(new DbaTableCopyPageRequest(definition, null, 1) { MaxBytes = 1024 }));

            Assert.Contains("variable-size native type", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await TryExecuteAsync(connection, $"DROP TABLE IF EXISTS \"{table}\"");
        }
    }

    [Theory]
    [Trait("Category", "LiveProvider")]
    [InlineData(DbaTableCopyProvider.PostgreSql, "DBACLIENTX_POSTGRESQL_TEST_CONNECTION")]
    [InlineData(DbaTableCopyProvider.MySql, "DBACLIENTX_MYSQL_TEST_CONNECTION")]
    public async Task ResumableCopy_KeysetRead_AndFailedPageRollback_AreProviderBacked(
        DbaTableCopyProvider provider,
        string environmentVariable)
    {
        var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {environmentVariable} to an isolated provider database.");

        var suffix = Guid.NewGuid().ToString("N");
        var destinationTable = "dbax_copy_" + suffix;
        var rollbackTable = "dbax_rollback_" + suffix;
        var sqlitePath = Path.Combine(Path.GetTempPath(), "dbax-live-" + suffix + ".sqlite");
        await using var connection = CreateConnection(provider, connectionString!);
        await connection.OpenAsync();
        try
        {
            await ExecuteAsync(connection, CreateTableSql(provider, destinationTable));
            await ExecuteAsync(connection, CreateTableSql(provider, rollbackTable));

            await VerifyAtomicRollbackAsync(provider, connectionString!, connection, rollbackTable);

            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sqlitePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, Payload TEXT NOT NULL)");
                sqlite.ExecuteNonQuery(sqlitePath, "INSERT INTO SourceRows VALUES (1, 'One'), (2, 'Dwa'), (3, '三')");
            }

            var source = new SQLiteTableCopyAdapter(sqlitePath, new[] { "Id" });
            var destination = CreateAdapter(provider, connectionString!);
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                destinationTable,
                new[] { "Id" })
            {
                UseKeysetPagination = true
            };
            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { definition },
                new DbaTableCopyOptions
                {
                    CheckpointId = "live-" + suffix,
                    PageSize = 1,
                    VerifyContent = true
                });

            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
            Assert.True((await destination.ReadCheckpointAsync(definition))!.Completed);
            if (provider == DbaTableCopyProvider.MySql)
            {
                Assert.Equal(
                    "InnoDB",
                    Convert.ToString(await ExecuteScalarAsync(
                        connection,
                        "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'DbaClientX_TableCopyCheckpoints'")));
            }

            var providerSource = CreateAdapter(
                provider,
                connectionString!,
                new[] { "id" },
                DbaTableCopyReadConsistency.Snapshot);
            var providerDefinition = new DbaTableCopyDefinition(
                destinationTable,
                destinationTable,
                new[] { "id" })
            {
                UseKeysetPagination = true
            };
            using (await (providerSource is IDbaTableCopyDefinitionReadSession definitionSession
                ? definitionSession.OpenReadSessionAsync(new[] { providerDefinition })
                : ((IDbaTableCopyReadSession)providerSource).OpenReadSessionAsync()))
            {
                using var first = await providerSource.ReadPageAsync(new(providerDefinition, null, 2));
                using var second = await providerSource.ReadPageAsync(new(providerDefinition, first.ContinuationToken, 2));
                Assert.Equal(2, first.Data.Rows.Count);
                Assert.Single(second.Data.Rows.Cast<DataRow>());
            }
        }
        finally
        {
            await TryExecuteAsync(connection, DropTableSql(provider, destinationTable));
            await TryExecuteAsync(connection, DropTableSql(provider, rollbackTable));
            await TryExecuteAsync(connection, DeleteCheckpointSql(provider, suffix));
            File.Delete(sqlitePath);
            File.Delete(sqlitePath + "-wal");
            File.Delete(sqlitePath + "-shm");
        }
    }

    private static async Task VerifyAtomicRollbackAsync(
        DbaTableCopyProvider provider,
        string connectionString,
        DbConnection connection,
        string table)
    {
        var destination = CreateAdapter(provider, connectionString);
        var definition = new DbaTableCopyDefinition("unused", table, new[] { "id" });
        var checkpoint = new DbaTableCopyCheckpoint
        {
            CopyId = "rollback-" + table.Substring(table.LastIndexOf('_') + 1),
            DefinitionFingerprint = new string('1', 64),
            SourceRows = 2,
            SourceContentHash = new string('2', 64),
            CopiedRows = 0,
            CopiedContentHash = new string('3', 64),
            Completed = false
        };
        await destination.InitializeCheckpointAsync(definition, checkpoint, clearDestination: false);

        using var page = new DataTable();
        page.Columns.Add("id", typeof(long));
        page.Columns.Add("payload", typeof(string));
        page.Rows.Add(1L, "first");
        page.Rows.Add(1L, "duplicate");

        await Assert.ThrowsAnyAsync<Exception>(() => destination.CommitPageAsync(
            definition,
            page,
            new DbaTableCopyOptions { CheckpointId = checkpoint.CopyId, BatchSize = 1 },
            checkpoint,
            checkpoint with { CopiedRows = 2 }));

        Assert.Equal(0L, Convert.ToInt64(await ExecuteScalarAsync(connection, CountSql(provider, table))));
        Assert.Equal(checkpoint, await destination.ReadCheckpointAsync(definition));
    }

    private static DbaTableCopyCheckpoint CreateInitialCheckpoint(string copyId)
        => new()
        {
            CopyId = copyId,
            DefinitionFingerprint = new string('1', 64),
            SourceRows = 0,
            SourceContentHash = new string('2', 64),
            CopiedRows = 0,
            CopiedContentHash = new string('3', 64),
            Completed = false
        };

    private static DbaProviderTableCopyAdapterBase CreateAdapter(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? orderBy = null,
        DbaTableCopyReadConsistency readConsistency = DbaTableCopyReadConsistency.CallerManaged)
    {
        var options = new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = connectionString,
            DefaultOrderByColumns = orderBy,
            ReadConsistency = readConsistency
        };
        return provider switch
        {
            DbaTableCopyProvider.PostgreSql => new PostgreSqlTableCopyAdapter(options),
            DbaTableCopyProvider.MySql => new MySqlTableCopyAdapter(options),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };
    }

    private static DbConnection CreateConnection(DbaTableCopyProvider provider, string connectionString)
        => provider switch
        {
            DbaTableCopyProvider.PostgreSql => new NpgsqlConnection(connectionString),
            DbaTableCopyProvider.MySql => new MySqlConnection(connectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };

    private static string CreateTableSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"CREATE TABLE \"{table}\" (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)"
            : $"CREATE TABLE `{table}` (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)";

    private static string DropTableSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"DROP TABLE IF EXISTS \"{table}\""
            : $"DROP TABLE IF EXISTS `{table}`";

    private static string CountSql(DbaTableCopyProvider provider, string table)
        => provider == DbaTableCopyProvider.PostgreSql
            ? $"SELECT COUNT(*) FROM \"{table}\""
            : $"SELECT COUNT(*) FROM `{table}`";

    private static string DeleteCheckpointSql(DbaTableCopyProvider provider, string suffix)
    {
        var table = provider == DbaTableCopyProvider.PostgreSql
            ? "\"DbaClientX_TableCopyCheckpoints\""
            : "`DbaClientX_TableCopyCheckpoints`";
        return $"DELETE FROM {table} WHERE CopyId LIKE '%{suffix.Replace("'", "''")}'";
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task TryExecuteAsync(DbConnection connection, string sql)
    {
        try
        {
            await ExecuteAsync(connection, sql);
        }
        catch (DbException)
        {
            // Cleanup is best-effort so it cannot hide the provider assertion that failed.
        }
    }

    private static async Task<object?> ExecuteScalarAsync(DbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}
