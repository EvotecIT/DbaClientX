using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryCompoundSequenceTests
{
    [Theory]
    [InlineData("NEXT VALUE FOR SequenceName")]
    [InlineData("PREVIOUS VALUE FOR `SequenceName`")]
    [InlineData("NEXT VALUE FOR `database`.`SequenceName`")]
    [InlineData("PREVIOUS VALUE FOR database.SequenceName")]
    [InlineData("NEXTVAL(database.SequenceName)")]
    [InlineData("LASTVAL(`database`.`SequenceName`)")]
    public void MariaDbSequenceProjection_IsAnonymousUnlessAliased(string expression)
    {
        string anonymous = BuildSequenceQuery(expression).Compile(SqlDialect.MySql);
        Assert.Contains("SELECT `dbx_column_0`, `dbx_column_1` AS `divisor`", anonymous);
        foreach (string alias in new[] { " OutputValue", " AS OutputValue" })
        {
            string named = BuildSequenceQuery(expression + alias).Compile(SqlDialect.MySql);
            Assert.DoesNotContain("WITH ", named);
        }
    }

    [Theory]
    [InlineData("NEXT VALUE FOR database.SequenceName + outer_row.Id")]
    [InlineData("NEXTVAL(database.SequenceName) + outer_row.Id")]
    public void MariaDbSequenceObject_DoesNotHideOuterColumnReference(string expression)
    {
        var exception = Assert.Throws<NotSupportedException>(() => BuildSequenceQuery(expression).Compile(SqlDialect.MySql));
        Assert.Contains("outer_row", exception.Message);
    }

    [Theory]
    [InlineData("next", "bare")]
    [InlineData("next", "quoted")]
    [InlineData("next", "qualified")]
    [InlineData("previous", "bare")]
    [InlineData("previous", "quoted")]
    [InlineData("previous", "qualified")]
    [InlineData("nextval", "qualified")]
    [InlineData("lastval", "qualified")]
    [Trait("Category", "LiveProvider")]
    public async Task MariaDbSequenceProjection_RemainsComposable(string operation, string nameShape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MariaDB test database.");
        using var client = new DBAClientX.MySql();
        string version = Convert.ToString(await client.ExecuteScalarAsync(connection!, "SELECT VERSION();"))!;
        Assert.SkipWhen(!version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase), "Sequences are specific to MariaDB.");
        string sequence = "dbx_sequence_" + Guid.NewGuid().ToString("N");
        bool created = false;
        try
        {
            await client.ExecuteNonQueryAsync(connection!, $"CREATE SEQUENCE `{sequence}` START WITH 1;");
            created = true;
            string database = Convert.ToString(await client.ExecuteScalarAsync(connection!, "SELECT DATABASE();"))!;
            string name = nameShape switch
            {
                "bare" => sequence,
                "quoted" => SqlIdentifier.Quote(SqlDialect.MySql, sequence),
                "qualified" => SqlIdentifier.Quote(SqlDialect.MySql, database) + "." + SqlIdentifier.Quote(SqlDialect.MySql, sequence),
                _ => throw new ArgumentOutOfRangeException(nameof(nameShape))
            };
            string expression = operation switch
            {
                "next" => "NEXT VALUE FOR " + name,
                "previous" => "PREVIOUS VALUE FOR " + name,
                "nextval" => "NEXTVAL(" + name + ")",
                "lastval" => "LASTVAL(" + name + ")",
                _ => throw new ArgumentOutOfRangeException(nameof(operation))
            };
            var query = new Query().Select("q.dbx_column_0").From(BuildSequenceQuery(expression), "q");
            var rows = await client.QueryAsListAsync(connection!, query.Compile(SqlDialect.MySql),
                row => row.IsDBNull(0) ? (long?)null : Convert.ToInt64(row.GetValue(0)));
            Assert.Equal(0L, Assert.Single(rows));
        }
        finally
        {
            if (created) await client.ExecuteNonQueryAsync(connection!, $"DROP SEQUENCE `{sequence}`;");
        }
    }

    private static Query BuildSequenceQuery(string expression)
        => new Query().SelectRaw(expression + ", 2 AS divisor")
            // A stable intersected branch avoids depending on how often the server evaluates the sequence.
            .Union(new Query().SelectRaw("0, 2")).Intersect(new Query().SelectRaw("0, 2"));
}
