using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core;

internal sealed class FastStreamWrapper
{
    private readonly Stream _wrappedStream;
    private readonly byte[] _buffer = new byte[32768]; //  2^15
    private int _count;
    private int _next;

    public FastStreamWrapper(Stream s)
    {
        _wrappedStream = s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<int> ReadByteAsync(CancellationToken cancelToken)
    {
        // fast path first
        if (_next < _count)
            return new ValueTask<int>(_buffer[_next++]);

        return ReadByteSlowAsync(cancelToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
