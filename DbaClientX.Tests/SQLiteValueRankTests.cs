using System.Data;
using DBAClientX;
using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLiteValueRankTests
{
    private static readonly string[] Declared = { "validation-failed", "act", "confirm", "O'Reilly --", "A", "a", " Ready ", "x'); DROP TABLE items; --" };
    private static readonly string?[] Values = {
        "validation-failed", "act", "confirm", "smtp-starttls-25", "A", "a", " Ready ", "Ready", "ready", " ",
        "O'Reilly --", "__proto__", "constructor", "\U00010000", "\uE000", "unknown", "act", "A", null, "x'); DROP TABLE items; --"
    };

    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    public void OffsetAndKeysetWalks_MatchCompleteOrdinalOrderWithoutSkips(bool descending, int pageSize)
    {
        using var fixture = new Fixture();
        var rank = SqlValueRank.Ordinal("state", Declared);
        var source = Source(rank);
        var keyset = Paging(rank, pageSize, descending);
        var expected = Ordered(descending).Select(row => row.Id).ToArray();
        var seen = new List<long>();
        string? cursor = null;
        do
        {
            var table = fixture.Read(keyset.CreatePageQuery(source, cursor));
            foreach (DataRow row in table.Rows) Assert.IsType<long>(row["rank.alias"]);
            var page = keyset.CreatePage(table);
            seen.AddRange(page.Items.Select(row => (long)row["id"]));
            cursor = page.NextCursor;
            Assert.InRange(seen.Count, 1, Values.Length);
        } while (cursor != null);
        Assert.Equal(expected, seen);
        Assert.Equal(seen.Count, seen.Distinct().Count());

        var ordered = Source(rank).OrderByValueRank(rank, descending)
            .OrderByRaw("\"state\" COLLATE DBX_ORDINAL" + (descending ? " DESC" : ""))
            .OrderByDescending("secondary").OrderBy("id");
        var offset = new OffsetPagination(pageSize);
        seen.Clear();
        cursor = null;
        do
        {
            var page = offset.CreatePage(fixture.Read(offset.CreatePageQuery(ordered, cursor)), cursor);
            seen.AddRange(page.Items.Select(row => (long)row["id"]));
            cursor = page.NextCursor;
            Assert.InRange(seen.Count, 1, Values.Length);
        } while (cursor != null);
        Assert.Equal(expected, seen);
        Assert.Empty(source.OrderByColumns);
        Assert.Null(source.LimitValue);
        Assert.Null(ordered.OffsetValue);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RowsBefore_IdOnlySeekQueryBindsRankWithoutSelectingRankProjection(bool descending)
    {
        using var fixture = new Fixture();
        var rank = SqlValueRank.Ordinal("state", Declared);
        var paging = Paging(rank, 100, descending);
        var ordered = Ordered(descending).ToArray();
        var boundary = ordered[4];
        var cursor = paging.CreateCursor(new object?[] { (long)Rank(boundary.Token), boundary.Token, boundary.Secondary, boundary.Id });
        var idOnly = new Query().Select("id").From("items").Where("tenant", 1).WhereNotNull("state");
        var query = paging.CreatePageQuery(idOnly, cursor);
        var compiled = query.CompileWithNamedParameters(SqlDialect.SQLite);

        Assert.StartsWith("SELECT \"id\" FROM", compiled.Sql);
        Assert.DoesNotContain("AS \"rank.alias\"", compiled.Sql);
        var table = fixture.Read(query);
        Assert.Single(table.Columns.Cast<DataColumn>());
        Assert.Equal(ordered.Skip(5).Select(row => row.Id), table.Rows.Cast<DataRow>().Select(row => (long)row["id"]));
        // RowsBefore-style count wraps an id-only seek source: rank is needed in WHERE/ORDER, never as an output field.
        var countQuery = new Query().SelectRaw("COUNT(*) AS \"remaining\"").From(query, "remaining_ids");
        Assert.Equal((long)ordered.Length - 5, fixture.Read(countQuery).Rows[0]["remaining"]);
        Assert.Equal(new[] { "id" }, idOnly.SelectColumns);
        Assert.Empty(idOnly.OrderByColumns);
    }

    [Fact]
    public void NoCaseStorage_KeepsCaseWhitespaceQuotedAndPrototypeTokensOrdinal()
    {
        using var fixture = new Fixture();
        var rank = SqlValueRank.Ordinal("state", Declared);
        var table = fixture.Read(new Query().Select("id", "state").SelectValueRank(rank, "rank").From("items").Where("tenant", 1).OrderBy("id"));
        Assert.Equal(Values.Length, table.Rows.Count);
        for (var index = 0; index < Values.Length; index++)
        {
            Assert.Equal((long)Rank(Values[index]), Assert.IsType<long>(table.Rows[index]["rank"]));
            Assert.Equal(Values[index] == null ? DBNull.Value : Values[index], table.Rows[index]["state"]);
        }
        Assert.Equal(3L, fixture.Scalar("SELECT COUNT(*) FROM items WHERE tenant=1 AND state=@value", "A"));
        Assert.NotEqual(table.Rows[4]["rank"], table.Rows[5]["rank"]);
        Assert.NotEqual(table.Rows[6]["rank"], table.Rows[7]["rank"]);
        Assert.Equal((long)Values.Length + 1, fixture.Scalar("SELECT COUNT(*) FROM items"));
    }

    [Fact]
    public void Cache_ChangedSameLengthDeclarationExecutesWithFreshSelectOrderAndSeekBinds()
    {
        using var fixture = new Fixture();
        var first = SqlValueRank.Ordinal("state", new[] { "act", "confirm" });
        var second = SqlValueRank.Ordinal("state", new[] { "confirm", "act" });
        Query OrderedQuery(SqlValueRank rank) => new Query().Select("id").SelectValueRank(rank, "rank").From("items")
            .Where("tenant", 1).WhereNotNull("state").OrderByValueRank(rank).OrderBy("id");
        var firstTable = fixture.Read(OrderedQuery(first));
        var secondTable = fixture.Read(OrderedQuery(second));
        Assert.Equal(2L, firstTable.Rows[0]["id"]);
        Assert.Equal(3L, secondTable.Rows[0]["id"]);
        Assert.Equal(0L, secondTable.Rows[0]["rank"]);

        var firstPaging = Paging(first, 2, false);
        var secondPaging = Paging(second, 2, false);
        var firstSource = Source(first);
        var firstPage = firstPaging.CreatePage(fixture.Read(firstPaging.CreatePageQuery(firstSource)));
        Assert.NotNull(firstPage.NextCursor);
        Assert.Throws<ArgumentException>(() => secondPaging.CreatePageQuery(Source(second), firstPage.NextCursor));
        var fresh = secondPaging.CreatePage(fixture.Read(secondPaging.CreatePageQuery(Source(second))));
        Assert.Equal(3L, fresh.Items[0]["id"]);
        var continued = secondPaging.CreatePage(fixture.Read(secondPaging.CreatePageQuery(Source(second), fresh.NextCursor)));
        Assert.DoesNotContain((long)continued.Items[0]["id"], fresh.Items.Select(row => (long)row["id"]));
    }

    [Fact]
    public void OrdinalCollation_OrdersSupplementaryBeforeBmpAndDiffersFromUtf8BinaryOrder()
    {
        using var fixture = new Fixture();
        Query Source() => new Query().Select("state").From("items").WhereIn("state", "\U00010000", "\uE000");
        var ordinal = fixture.Read(Source().OrderByRaw("\"state\" COLLATE DBX_ORDINAL"));
        var binary = fixture.Read(Source().OrderByRaw("\"state\" COLLATE BINARY"));
        Assert.Equal(new[] { "\U00010000", "\uE000" }, ordinal.Rows.Cast<DataRow>().Select(row => (string)row["state"]));
        Assert.Equal(new[] { "\uE000", "\U00010000" }, binary.Rows.Cast<DataRow>().Select(row => (string)row["state"]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NullableOffset_UsesExplicitNullLastAndKeepsNullsRaw(bool descending)
    {
        using var fixture = new Fixture();
        var rank = SqlValueRank.Ordinal("state", Declared);
        var query = new Query().Select("id", "state").SelectValueRank(rank, "rank").From("items").Where("tenant", 1)
            .OrderByRaw("\"state\" IS NULL").OrderByValueRank(rank, descending)
            .OrderByRaw("\"state\" COLLATE DBX_ORDINAL" + (descending ? " DESC" : ""))
            .OrderByDescending("secondary").OrderBy("id");
        var table = fixture.Read(new OffsetPagination(100).CreatePageQuery(query));
        Assert.Equal(Ordered(descending).Select(row => row.Id).Append(19L), table.Rows.Cast<DataRow>().Select(row => (long)row["id"]));
        Assert.Equal(DBNull.Value, table.Rows[table.Rows.Count - 1]["state"]);
        Assert.Equal((long)Declared.Length, table.Rows[table.Rows.Count - 1]["rank"]);
        Assert.Throws<InvalidOperationException>(() => Paging(rank, 2, false).CreateCursor(new object?[] { (long)Declared.Length, null, 0L, 19L }));
    }

    [Fact]
    public void OrdinalRegistration_RespectsActiveReaderAndCloseReopenWithoutChangingNoCase()
    {
        using var fixture = new Fixture();
        using (var command = fixture.Connection.CreateCommand())
        {
            command.CommandText = "SELECT state FROM items ORDER BY state COLLATE DBX_ORDINAL";
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Throws<InvalidOperationException>(() => SQLiteUnicodeText.Register(fixture.Connection));
        }
        Assert.Equal(1L, fixture.Scalar("SELECT 'A' COLLATE DBX_NOCASE = 'a'"));
        Assert.Equal(0L, fixture.Scalar("SELECT 'A' COLLATE DBX_ORDINAL = 'a'"));
        fixture.Connection.Close();
        fixture.Connection.Open();
        SQLiteUnicodeText.Register(fixture.Connection);
        Assert.Equal(1L, fixture.Scalar("SELECT @value COLLATE DBX_ORDINAL < char(57344)", "\U00010000"));
        Assert.Equal(1L, fixture.Scalar("SELECT 'A' COLLATE DBX_NOCASE = 'a'"));
    }

    private static int Rank(string? token)
    {
        var index = Array.FindIndex(Declared, value => string.Equals(value, token, StringComparison.Ordinal));
        return index >= 0 ? index : Declared.Length;
    }

    private static IEnumerable<(long Id, string Token, long Secondary)> Ordered(bool descending)
    {
        var rows = Values.Select((token, index) => (Id: (long)index + 1, Token: token!, Secondary: (long)(index % 3))).Where(row => row.Token != null);
        return descending ? rows.OrderByDescending(row => Rank(row.Token)).ThenByDescending(row => row.Token, StringComparer.Ordinal).ThenByDescending(row => row.Secondary).ThenBy(row => row.Id)
            : rows.OrderBy(row => Rank(row.Token)).ThenBy(row => row.Token, StringComparer.Ordinal).ThenByDescending(row => row.Secondary).ThenBy(row => row.Id);
    }

    private static Query Source(SqlValueRank rank) => new Query().Select("id", "secondary").SelectRaw("\"state\" AS \"raw.state\"")
        .SelectValueRank(rank, "rank.alias").From("items").Where("tenant", 1).WhereNotNull("state");

    private static KeysetPagination Paging(SqlValueRank rank, int size, bool descending) => new(size, new[] {
        KeysetColumn.ValueRank(rank, "rank.alias", descending),
        new KeysetColumn("state", descending, "raw.state", typeof(string)).WithCollation(SQLiteUnicodeText.OrdinalCollation),
        new KeysetColumn("secondary", descending: true, valueType: typeof(long))
    }, KeysetColumn.Asc<long>("id"));

    private sealed class Fixture : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), "dbx-value-rank-" + Guid.NewGuid().ToString("N") + ".db");
        internal SqliteConnection Connection { get; }

        internal Fixture()
        {
            Connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = _path, Pooling = false }.ToString());
            Connection.Open();
            SQLiteUnicodeText.Register(Connection);
            using var create = Connection.CreateCommand();
            create.CommandText = "CREATE TABLE items(id INTEGER PRIMARY KEY, state TEXT COLLATE NOCASE, secondary INTEGER NOT NULL, tenant INTEGER NOT NULL)";
            create.ExecuteNonQuery();
            using var insert = Connection.CreateCommand();
            insert.CommandText = "INSERT INTO items VALUES(@id,@state,@secondary,@tenant)";
            var id = insert.Parameters.Add("@id", SqliteType.Integer);
            var state = insert.Parameters.Add("@state", SqliteType.Text);
            var secondary = insert.Parameters.Add("@secondary", SqliteType.Integer);
            var tenant = insert.Parameters.Add("@tenant", SqliteType.Integer);
            for (var index = 0; index <= Values.Length; index++)
            {
                id.Value = index + 1;
                state.Value = index == Values.Length ? "act" : (object?)Values[index] ?? DBNull.Value;
                secondary.Value = index % 3;
                tenant.Value = index == Values.Length ? 2 : 1;
                insert.ExecuteNonQuery();
            }
        }

        internal DataTable Read(Query query)
        {
            var (sql, parameters) = query.CompileWithNamedParameters(SqlDialect.SQLite);
            using var command = Connection.CreateCommand();
            command.CommandText = sql;
            foreach (var parameter in parameters) command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
            using var reader = command.ExecuteReader();
            var table = new DataTable();
            table.Load(reader);
            return table;
        }

        internal object? Scalar(string sql, string? value = null)
        {
            using var command = Connection.CreateCommand();
            command.CommandText = sql;
            if (value != null) command.Parameters.AddWithValue("@value", value);
            return command.ExecuteScalar();
        }

        public void Dispose()
        {
            Connection.Dispose();
            File.Delete(_path);
        }
    }
}
