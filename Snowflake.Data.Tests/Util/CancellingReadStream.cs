using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Util;

/// <summary>
/// Stream wrapper that cancels the provided CTS after reading a threshold number of bytes,
/// then returns a canceled task on subsequent reads.
/// </summary>
internal sealed class CancellingReadStream : Stream
{
    private readonly byte[] _data;
    private readonly CancellationTokenSource _cts;
    private readonly int _bytesBeforeCancel;
    private int _position;

    public CancellingReadStream(byte[] data, CancellationTokenSource cts, int bytesBeforeCancel)
    {
        _data = data;
        _cts = cts;
        _bytesBeforeCancel = bytesBeforeCancel;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_cts.IsCancellationRequested)
            throw new OperationCanceledException(_cts.Token);

        if (_position >= _data.Length)
            return 0;

        var toRead = Math.Min(count, _data.Length - _position);
        Array.Copy(_data, _position, buffer, offset, toRead);
        _position += toRead;

        if (_position >= _bytesBeforeCancel)
            _cts.Cancel();

        return toRead;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled<int>(cancellationToken)
            : Task.FromResult(Read(buffer, offset, count));
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _data.Length;
    public override long Position { get => _position; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
