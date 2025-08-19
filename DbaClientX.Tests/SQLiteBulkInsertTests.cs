using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteBulkInsertTests
{
    private class CaptureSQLite : DBAClientX.SQLite
    {
        public List<string> Queries { get; } = new();
        public List<int> BatchRowCounts { get; } = new();

        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            Queries.Add(query);
            if (query.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                var valuesPart = query.Substring(query.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase));
                var rowCount = valuesPart.Count(c => c == '(');
                BatchRowCounts.Add(rowCount);
            }
            else
            {
                BatchRowCounts.Add(0);
            }
            return 0;
        }

        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var result = ExecuteNonQuery(connection, transaction, query, parameters, parameterTypes, parameterDirections);
            return Task.FromResult(result);
        }
    }

    [Fact]
    public void BulkInsert_BatchesRows()
    {
        using var sqlite = new CaptureSQLite();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        sqlite.BulkInsert(":memory:", table, "Dest", batchSize: 1);

        Assert.All(sqlite.Queries, q => Assert.Contains("INSERT INTO Dest", q));
        Assert.Equal(new[] { 1, 1 }, sqlite.BatchRowCounts);
    }

    [Fact]
    public async Task BulkInsertAsync_BatchesRows()
    {
        using var sqlite = new CaptureSQLite();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        await sqlite.BulkInsertAsync(":memory:", table, "Dest", batchSize: 1);

        Assert.All(sqlite.Queries, q => Assert.Contains("INSERT INTO Dest", q));
        Assert.Equal(new[] { 1, 1 }, sqlite.BatchRowCounts);
    }

    [Fact]
    public void BulkInsert_DefaultBatch_InsertsAllRows()
    {
        using var sqlite = new CaptureSQLite();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        sqlite.BulkInsert(":memory:", table, "Dest");

        Assert.Single(sqlite.BatchRowCounts);
        Assert.Equal(2, sqlite.BatchRowCounts[0]);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.BulkInsert(":memory:", table, "Dest", useTransaction: true));
    }
}

