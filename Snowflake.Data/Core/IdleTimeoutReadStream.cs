using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core;

internal sealed class IdleTimeoutReadStream : Stream
{
    private readonly Stream _inner;
    private readonly TimeSpan _idleTimeout;
    private readonly TimeSpan _readTimeout;
    private readonly Lazy<Timer> _idleTimer;
    private volatile bool _idleExpired;

    public IdleTimeoutReadStream(Stream inner, TimeSpan idleTimeout, TimeSpan readTimeout)
    {
        _inner = inner;
        _idleTimeout = idleTimeout;
        _readTimeout = readTimeout;
        _idleTimer = new Lazy<Timer>(() => new Timer(_ => _idleExpired = true, null, idleTimeout.Ticks == 0 ? Timeout.InfiniteTimeSpan : _idleTimeout, Timeout.InfiniteTimeSpan));
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_idleTimeout.Ticks <= 0 && _readTimeout.Ticks <= 0)
            return await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);

        if (_idleExpired)
            throw new TimeoutException($"No data received for {_idleTimeout.TotalSeconds} seconds");

        CancellationTokenSource readCts = null;
        var readToken = cancellationToken;
        if (_readTimeout.Ticks > 0)
        {
            readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            readCts.CancelAfter(_readTimeout);
            readToken = readCts.Token;
        }

        try
        {
            var bytesRead = await _inner.ReadAsync(buffer, offset, count, readToken).ConfigureAwait(false);
            ResetIdleTimer();
            return bytesRead;
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Read took longer than {_readTimeout.TotalSeconds} seconds", ex);
        }
        finally
        {
            readCts?.Dispose();
        }
    }

    private void ResetIdleTimer()
    {
        if (_idleTimeout.Ticks <= 0)
            return;

        _idleTimer.Value.Change(_idleTimeout, Timeout.InfiniteTimeSpan);
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        _inner.Read(buffer, offset, count);

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position { get => _inner.Position; set => _inner.Position = value; }
    public override void Flush() => _inner.Flush();
    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => _inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("This decorator has no writing capabilities!");

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (_idleTimer.IsValueCreated)
                _idleTimer.Value.Dispose();

            _inner.Dispose();
        }

        base.Dispose(disposing);
    }
}
