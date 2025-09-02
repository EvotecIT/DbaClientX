using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public class OracleBulkInsertTests
{
    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.BulkInsert("h", "svc", "u", "p", table, "Dest", useTransaction: true));
    }
}
