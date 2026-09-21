using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public sealed partial class DbaDataReader
{
    private sealed class NormalizingReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly Func<Exception, CancellationToken, Exception> _exceptionFactory;

        internal NormalizingReadStream(
            Stream inner,
            Func<Exception, CancellationToken, Exception> exceptionFactory)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _exceptionFactory = exceptionFactory ?? throw new ArgumentNullException(nameof(exceptionFactory));
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override bool CanTimeout => _inner.CanTimeout;
        public override int ReadTimeout { get => _inner.ReadTimeout; set => _inner.ReadTimeout = value; }
        public override int WriteTimeout { get => _inner.WriteTimeout; set => _inner.WriteTimeout = value; }

        public override void Flush() => _inner.Flush();

        public override int Read(byte[] buffer, int offset, int count)
            => Execute(() => _inner.Read(buffer, offset, count), CancellationToken.None);

        public override int ReadByte()
            => Execute(_inner.ReadByte, CancellationToken.None);

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
            => ExecuteAsync(() => _inner.ReadAsync(buffer, offset, count, cancellationToken), cancellationToken);

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer)
        {
            try
            {
                return _inner.Read(buffer);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, CancellationToken.None))
            {
                throw _exceptionFactory(exception, CancellationToken.None);
            }
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
            => ExecuteValueTaskAsync(() => _inner.ReadAsync(buffer, cancellationToken), cancellationToken);

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
#endif

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        private T Execute<T>(Func<T> operation, CancellationToken cancellationToken)
        {
            try
            {
                return operation();
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, cancellationToken))
            {
                throw _exceptionFactory(exception, cancellationToken);
            }
        }

        private async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, cancellationToken))
            {
                throw _exceptionFactory(exception, cancellationToken);
            }
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        private async ValueTask<T> ExecuteValueTaskAsync<T>(
            Func<ValueTask<T>> operation,
            CancellationToken cancellationToken)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, cancellationToken))
            {
                throw _exceptionFactory(exception, cancellationToken);
            }
        }
#endif
    }

    private sealed class NormalizingTextReader : TextReader
    {
        private readonly TextReader _inner;
        private readonly Func<Exception, CancellationToken, Exception> _exceptionFactory;

        internal NormalizingTextReader(
            TextReader inner,
            Func<Exception, CancellationToken, Exception> exceptionFactory)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _exceptionFactory = exceptionFactory ?? throw new ArgumentNullException(nameof(exceptionFactory));
        }

        public override int Peek() => Execute(_inner.Peek);
        public override int Read() => Execute(_inner.Read);
        public override int Read(char[] buffer, int index, int count)
            => Execute(() => _inner.Read(buffer, index, count));
        public override int ReadBlock(char[] buffer, int index, int count)
            => Execute(() => _inner.ReadBlock(buffer, index, count));
        public override string? ReadLine() => Execute(_inner.ReadLine);
        public override string ReadToEnd() => Execute(_inner.ReadToEnd);

        public override Task<int> ReadAsync(char[] buffer, int index, int count)
            => ExecuteAsync(() => _inner.ReadAsync(buffer, index, count), CancellationToken.None);

        public override Task<int> ReadBlockAsync(char[] buffer, int index, int count)
            => ExecuteAsync(() => _inner.ReadBlockAsync(buffer, index, count), CancellationToken.None);

        public override Task<string?> ReadLineAsync()
            => ExecuteAsync(_inner.ReadLineAsync, CancellationToken.None);

        public override Task<string> ReadToEndAsync()
            => ExecuteAsync(_inner.ReadToEndAsync, CancellationToken.None);

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<char> buffer)
        {
            try
            {
                return _inner.Read(buffer);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, CancellationToken.None))
            {
                throw _exceptionFactory(exception, CancellationToken.None);
            }
        }

        public override int ReadBlock(Span<char> buffer)
        {
            try
            {
                return _inner.ReadBlock(buffer);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, CancellationToken.None))
            {
                throw _exceptionFactory(exception, CancellationToken.None);
            }
        }

        public override ValueTask<int> ReadAsync(
            Memory<char> buffer,
            CancellationToken cancellationToken = default)
            => ExecuteValueTaskAsync(() => _inner.ReadAsync(buffer, cancellationToken), cancellationToken);

        public override ValueTask<int> ReadBlockAsync(
            Memory<char> buffer,
            CancellationToken cancellationToken = default)
            => ExecuteValueTaskAsync(() => _inner.ReadBlockAsync(buffer, cancellationToken), cancellationToken);
#endif

#if NET8_0_OR_GREATER
        public override ValueTask<string?> ReadLineAsync(CancellationToken cancellationToken)
            => ExecuteValueTaskAsync(() => _inner.ReadLineAsync(cancellationToken), cancellationToken);

        public override Task<string> ReadToEndAsync(CancellationToken cancellationToken)
            => ExecuteAsync(() => _inner.ReadToEndAsync(cancellationToken), cancellationToken);
#endif

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        private T Execute<T>(Func<T> operation)
        {
            try
            {
                return operation();
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, CancellationToken.None))
            {
                throw _exceptionFactory(exception, CancellationToken.None);
            }
        }

        private async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, cancellationToken))
            {
                throw _exceptionFactory(exception, cancellationToken);
            }
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        private async ValueTask<T> ExecuteValueTaskAsync<T>(
            Func<ValueTask<T>> operation,
            CancellationToken cancellationToken)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception exception) when (IsProviderConsumptionException(exception, cancellationToken))
            {
                throw _exceptionFactory(exception, cancellationToken);
            }
        }
#endif
    }
}
