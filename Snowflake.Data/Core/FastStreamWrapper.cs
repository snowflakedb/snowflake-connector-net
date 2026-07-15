using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core;

/// <summary>
/// Buffered wrapper around a <see cref="Stream"/> that provides efficient single-byte async reads.
/// Uses a 32 KB internal buffer to minimize calls to the underlying stream.
/// </summary>
internal sealed class FastStreamWrapper
{
    private readonly Stream _wrappedStream;
    private readonly byte[] _buffer = new byte[32768];
    private int _count;
    private int _next;

    /// <summary>
    /// Initializes a new instance wrapping the specified stream.
    /// </summary>
    /// <param name="s">The source stream to buffer reads from.</param>
    public FastStreamWrapper(Stream s)
    {
        _wrappedStream = s;
    }

    /// <summary>
    /// Reads a single byte from the buffered stream asynchronously.
    /// Returns -1 when the end of the stream is reached.
    /// </summary>
    /// <param name="cancelToken">Token to cancel the read operation.</param>
    /// <returns>The byte value or -1 if end of stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<int> ReadByteAsync(CancellationToken cancelToken)
    {
        // fast path first
        if (_next < _count)
            return new ValueTask<int>(_buffer[_next++]);

        return ReadByteSlowAsync(cancelToken);
    }

    private async ValueTask<int> ReadByteSlowAsync(CancellationToken cancelToken)
    {
        // fast path first
        if (_next < _count)
            return _buffer[_next++];

        if (_count >= 0)
        {
            _next = 0;
            _count = await _wrappedStream.ReadAsync(_buffer, 0, _buffer.Length, cancelToken).ConfigureAwait(false);
        }

        if (_count <= 0)
        {
            _count = -1;
            return -1;
        }

        return _buffer[_next++];
    }
}
