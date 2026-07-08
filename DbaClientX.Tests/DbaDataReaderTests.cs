using System;
using System.Data;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class DbaDataReaderTests
{
    [Fact]
    public void Dispose_DisposesReaderCommandAndOwnedConnectionOnce()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        using var sourceReader = table.CreateDataReader();
        using var connection = new SqlConnection();
        var command = new DisposableCommand();
        var connectionDisposeCount = 0;
        var afterReaderDisposedCount = 0;

        var reader = new DBAClientX.DbaDataReader(
            sourceReader,
            command,
            connection,
            ownsConnection: true,
            disposeConnection: resource =>
            {
                Assert.Same(connection, resource);
                connectionDisposeCount++;
            },
            afterReaderDisposed: () =>
            {
                Assert.True(sourceReader.IsClosed);
                afterReaderDisposedCount++;
            });

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));

        reader.Dispose();
        reader.Dispose();

        Assert.True(sourceReader.IsClosed);
        Assert.Equal(1, afterReaderDisposedCount);
        Assert.Equal(1, command.DisposeCount);
        Assert.Equal(1, connectionDisposeCount);
    }

    private sealed class DisposableCommand : IDisposable
    {
        public int DisposeCount { get; private set; }

        public void Dispose() => DisposeCount++;
    }
}
