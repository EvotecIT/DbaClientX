using System;
using System.Data;
using System.Data.Common;
using System.Collections;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class DbaDataReaderTests
{
    [Fact]
    public void ProviderAsyncReaderApis_ReturnOwnedReaderType()
    {
        var providerMethods = new[]
        {
            (typeof(DBAClientX.SqlServer), nameof(DBAClientX.SqlServer.QueryReaderAsync)),
            (typeof(DBAClientX.PostgreSql), nameof(DBAClientX.PostgreSql.QueryReaderAsync)),
            (typeof(DBAClientX.MySql), nameof(DBAClientX.MySql.QueryReaderAsync)),
            (typeof(DBAClientX.Oracle), nameof(DBAClientX.Oracle.QueryReaderAsync)),
            (typeof(DBAClientX.SQLite), nameof(DBAClientX.SQLite.QueryReaderAsync))
        };

        foreach (var (providerType, methodName) in providerMethods)
        {
            var methods = providerType.GetMethods().Where(method => method.Name == methodName).ToArray();
            Assert.NotEmpty(methods);
            Assert.All(methods, method => Assert.Equal(typeof(Task<DBAClientX.DbaDataReader>), method.ReturnType));
        }
    }

    [Fact]
    public async Task SQLiteQueryReaderAsync_StreamsParametersAndOwnsResources()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbax-reader-" + Guid.NewGuid().ToString("N") + ".sqlite");
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Rows (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL); INSERT INTO Rows VALUES (1, 'one'), (2, 'two');");

            await using var reader = await sqlite.QueryReaderAsync(
                path,
                "SELECT Id, Payload FROM Rows WHERE Id >= @id ORDER BY Id",
                new Dictionary<string, object?> { ["@id"] = 2 });

            Assert.True(await reader.ReadAsync(CancellationToken.None));
            Assert.Equal(2L, reader.GetInt64(0));
            Assert.Equal("two", reader.GetString(1));
            Assert.False(await reader.ReadAsync(CancellationToken.None));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + "-wal");
            File.Delete(path + "-shm");
        }
    }

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

    [Fact]
    public async Task ReadAsync_NormalizesDeferredProviderFailure()
    {
        const string rawMessage = "server=secret;password=hidden";
        await using var reader = new DBAClientX.DbaDataReader(
            new ThrowingDataReader(new TestProviderException(rawMessage)),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: (exception, _) =>
                new DBAClientX.DbaQueryExecutionException("Deferred read failed.", "SELECT sensitive", exception));

        var exception = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(
            () => reader.ReadAsync(CancellationToken.None));

        Assert.DoesNotContain(rawMessage, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT sensitive", exception.Message, StringComparison.Ordinal);
        Assert.Equal(64, exception.QueryFingerprint?.Length);
    }

    [Fact]
    public async Task NextResultAsync_PreservesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var expected = new OperationCanceledException("caller canceled", cancellation.Token);
        bool factoryCalled = false;
        await using var reader = new DBAClientX.DbaDataReader(
            new ThrowingDataReader(expected),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: (exception, token) =>
            {
                factoryCalled = true;
                return new OperationCanceledException("safe cancellation", exception, token);
            });

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(
            () => reader.NextResultAsync(cancellation.Token));

        Assert.Same(expected, exception);
        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task ReadAsync_NormalizesProviderCancellationWithoutCallerCancellation()
    {
        await using var reader = new DBAClientX.DbaDataReader(
            new ThrowingDataReader(new OperationCanceledException("provider canceled")),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: (exception, _) =>
                new DBAClientX.DbaQueryExecutionException("Deferred read failed.", "SELECT sensitive", exception));

        await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(
            () => reader.ReadAsync(CancellationToken.None));
    }

    [Fact]
    public void SynchronousFieldReads_NormalizeDeferredProviderFailures()
    {
        const string rawMessage = "server=secret;password=hidden";

        using var bytesReader = CreateNormalizingReader(rawMessage);
        var bytesException = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() =>
            bytesReader.GetBytes(0, 0, null, 0, 0));

        using var valueReader = CreateNormalizingReader(rawMessage);
        var valueException = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() =>
            valueReader.GetFieldValue<int>(0));

        Assert.DoesNotContain(rawMessage, bytesException.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(rawMessage, valueException.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(typeof(IndexOutOfRangeException))]
    [InlineData(typeof(InvalidCastException))]
    [InlineData(typeof(ObjectDisposedException))]
    public void SynchronousFieldReads_PreserveCallerContractErrors(Type exceptionType)
    {
        Exception expected = exceptionType == typeof(ObjectDisposedException)
            ? new ObjectDisposedException("reader", "caller contract")
            : (Exception)Activator.CreateInstance(exceptionType, "caller contract")!;
        using var reader = new DBAClientX.DbaDataReader(
            new ThrowingDataReader(expected, throwOnFieldAccess: true),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: (exception, _) =>
                new DBAClientX.DbaQueryExecutionException("Deferred field read failed.", "SELECT sensitive", exception));

        Exception actual = Assert.Throws(exceptionType, () => reader.GetValue(0));

        Assert.Same(expected, actual);
    }

    [Fact]
    public void DeferredStreamAndTextReads_NormalizeProviderFailures()
    {
        const string rawMessage = "server=secret;password=hidden";
        using var reader = CreateDeferredValueReader(
            new ThrowingStream(new TestProviderException(rawMessage)),
            new ThrowingTextReader(new TestProviderException(rawMessage)));
        using Stream stream = reader.GetStream(0);
        using TextReader textReader = reader.GetTextReader(0);

        var streamException = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => stream.ReadByte());
        var textException = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => textReader.Read());

        Assert.DoesNotContain(rawMessage, streamException.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(rawMessage, textException.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeferredStreamReadAsync_PreservesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var expected = new OperationCanceledException("caller canceled", cancellation.Token);
        bool factoryCalled = false;
        using var reader = CreateDeferredValueReader(
            new ThrowingStream(expected),
            TextReader.Null,
            (exception, token) =>
            {
                factoryCalled = true;
                return new OperationCanceledException("safe cancellation", exception, token);
            });
        await using Stream stream = reader.GetStream(0);

        OperationCanceledException exception = await Assert.ThrowsAsync<OperationCanceledException>(
            () => stream.ReadAsync(new byte[1], 0, 1, cancellation.Token));

        Assert.Same(expected, exception);
        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task DeferredMemoryStreamReadAsync_PreservesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var expected = new OperationCanceledException("caller canceled", cancellation.Token);
        bool factoryCalled = false;
        using var reader = CreateDeferredValueReader(
            new ThrowingStream(expected),
            TextReader.Null,
            (exception, token) =>
            {
                factoryCalled = true;
                return new OperationCanceledException("safe cancellation", exception, token);
            });
        await using Stream stream = reader.GetStream(0);

        OperationCanceledException exception = await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await stream.ReadExactlyAsync(new Memory<byte>(new byte[1]), cancellation.Token));

        Assert.Same(expected, exception);
        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task DeferredMemoryStreamReadAsync_NormalizesProviderCancellation()
    {
        var expected = new OperationCanceledException("provider canceled");
        using var reader = CreateDeferredValueReader(
            new ThrowingStream(expected),
            TextReader.Null,
            (exception, token) => new OperationCanceledException("safe cancellation", exception, token));
        await using Stream stream = reader.GetStream(0);

        OperationCanceledException exception = await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await stream.ReadExactlyAsync(new Memory<byte>(new byte[1]), CancellationToken.None));

        Assert.Equal("safe cancellation", exception.Message);
        Assert.Same(expected, exception.InnerException);
    }

    private static DBAClientX.DbaDataReader CreateNormalizingReader(string rawMessage)
        => new(
            new ThrowingDataReader(new TestProviderException(rawMessage), throwOnFieldAccess: true),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: (exception, _) =>
                new DBAClientX.DbaQueryExecutionException("Deferred field read failed.", "SELECT sensitive", exception));

    private static DBAClientX.DbaDataReader CreateDeferredValueReader(
        Stream stream,
        TextReader textReader,
        Func<Exception, CancellationToken, Exception>? exceptionFactory = null)
        => new(
            new ThrowingDataReader(
                new TestProviderException("unused"),
                stream: stream,
                textReader: textReader),
            command: null,
            connection: null,
            ownsConnection: false,
            disposeConnection: null,
            afterReaderDisposed: null,
            disposeConnectionAsync: null,
            afterReaderDisposedAsync: null,
            consumptionExceptionFactory: exceptionFactory ?? ((exception, _) =>
                new DBAClientX.DbaQueryExecutionException("Deferred value read failed.", "SELECT sensitive", exception)));

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

    private sealed class ThrowingDataReader : DbDataReader
    {
        private readonly DataTableReader _inner = new DataTable().CreateDataReader();
        private readonly Exception _exception;
        private readonly bool _throwOnFieldAccess;
        private readonly Stream? _stream;
        private readonly TextReader? _textReader;

        public ThrowingDataReader(
            Exception exception,
            bool throwOnFieldAccess = false,
            Stream? stream = null,
            TextReader? textReader = null)
        {
            _exception = exception;
            _throwOnFieldAccess = throwOnFieldAccess;
            _stream = stream;
            _textReader = textReader;
        }

        public override int FieldCount => _inner.FieldCount;
        public override bool HasRows => _inner.HasRows;
        public override bool IsClosed => _inner.IsClosed;
        public override int Depth => _inner.Depth;
        public override int RecordsAffected => _inner.RecordsAffected;
        public override object this[int ordinal] => _inner[ordinal];
        public override object this[string name] => _inner[name];
        public override bool Read() => throw _exception;
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromException<bool>(_exception);
        public override bool NextResult() => throw _exception;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromException<bool>(_exception);
        public override string GetName(int ordinal) => _inner.GetName(ordinal);
        public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
        public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);
        public override object GetValue(int ordinal)
            => _throwOnFieldAccess ? throw _exception : _inner.GetValue(ordinal);
        public override int GetValues(object[] values) => _inner.GetValues(values);
        public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
        public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
            => _throwOnFieldAccess
                ? throw _exception
                : _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => _inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
            => _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
        public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
        public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
        public override string GetString(int ordinal) => _inner.GetString(ordinal);
        public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
        public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
        public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);
        public override Stream GetStream(int ordinal) => _stream ?? base.GetStream(ordinal);
        public override TextReader GetTextReader(int ordinal) => _textReader ?? base.GetTextReader(ordinal);
        public override IEnumerator GetEnumerator() => ((IEnumerable)_inner).GetEnumerator();
        public override void Close() => _inner.Close();
        public override DataTable? GetSchemaTable() => _inner.GetSchemaTable();
    }

    private sealed class TestProviderException : DbException
    {
        public TestProviderException(string message) : base(message)
        {
        }
    }

    private sealed class ThrowingStream : Stream
    {
        private readonly Exception _exception;

        public ThrowingStream(Exception exception) => _exception = exception;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw _exception;
        public override int ReadByte() => throw _exception;
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => Task.FromException<int>(_exception);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class ThrowingTextReader : TextReader
    {
        private readonly Exception _exception;

        public ThrowingTextReader(Exception exception) => _exception = exception;

        public override int Read() => throw _exception;
        public override Task<int> ReadAsync(char[] buffer, int index, int count)
            => Task.FromException<int>(_exception);
    }
}
