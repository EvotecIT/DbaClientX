using System;
using System.Data;
using System.Data.Common;
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

    [Fact]
    public void Close_DisposesReaderCommandAndOwnedConnectionOnce()
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

        reader.Close();
        reader.Close();
        reader.Dispose();

        Assert.True(sourceReader.IsClosed);
        Assert.Equal(1, afterReaderDisposedCount);
        Assert.Equal(1, command.DisposeCount);
        Assert.Equal(1, connectionDisposeCount);
    }

    [Fact]
    public async Task DisposeAsync_UsesAsyncResourcesAndIsIdempotent()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var sourceReader = table.CreateDataReader();
        var connection = new SqlConnection();
        var command = new DisposableCommand();
        var syncConnectionDisposeCount = 0;
        var asyncConnectionDisposeCount = 0;
        var syncAfterReaderDisposedCount = 0;
        var asyncAfterReaderDisposedCount = 0;

        var reader = new DBAClientX.DbaDataReader(
            sourceReader,
            command,
            connection,
            ownsConnection: true,
            disposeConnection: _ => syncConnectionDisposeCount++,
            afterReaderDisposed: () => syncAfterReaderDisposedCount++,
            disposeConnectionAsync: resource =>
            {
                Assert.Same(connection, resource);
                asyncConnectionDisposeCount++;
                return default;
            },
            afterReaderDisposedAsync: () =>
            {
                Assert.True(sourceReader.IsClosed);
                asyncAfterReaderDisposedCount++;
                return default;
            });

        Assert.IsAssignableFrom<DbDataReader>(reader);
        Assert.True(reader.HasRows);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        Assert.Equal(1, await reader.GetFieldValueAsync<int>(0, CancellationToken.None));

        await reader.DisposeAsync();
        await reader.DisposeAsync();
        reader.Dispose();

        Assert.True(sourceReader.IsClosed);
        Assert.Equal(0, command.DisposeCount);
        Assert.Equal(1, command.AsyncDisposeCount);
        Assert.Equal(0, syncAfterReaderDisposedCount);
        Assert.Equal(1, asyncAfterReaderDisposedCount);
        Assert.Equal(0, syncConnectionDisposeCount);
        Assert.Equal(1, asyncConnectionDisposeCount);
    }

    [Fact]
    public async Task DisposeAsync_PreservesSynchronousPostReaderCallback()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var sourceReader = table.CreateDataReader();
        var command = new DisposableCommand();
        var afterReaderDisposedCount = 0;

        var reader = new DBAClientX.DbaDataReader(
            sourceReader,
            command,
            afterReaderDisposed: () =>
            {
                Assert.True(sourceReader.IsClosed);
                afterReaderDisposedCount++;
            });

        await reader.DisposeAsync();

        Assert.Equal(1, afterReaderDisposedCount);
        Assert.Equal(0, command.DisposeCount);
        Assert.Equal(1, command.AsyncDisposeCount);
    }

    private sealed class DisposableCommand : IDisposable, IAsyncDisposable
    {
        public int DisposeCount { get; private set; }

        public int AsyncDisposeCount { get; private set; }

        public void Dispose() => DisposeCount++;

        public ValueTask DisposeAsync()
        {
            AsyncDisposeCount++;
            return default;
        }
    }
}
