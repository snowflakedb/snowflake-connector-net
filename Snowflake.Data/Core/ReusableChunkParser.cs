using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Snowflake.Data.Core;

using Snowflake.Data.Client;
using System.Threading.Tasks;

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
    public async Task<int> ReadByteAsync(CancellationToken cancelToken)
    {
        // fast path first
        if (_next < _count)
            return _buffer[_next++];

        return await ReadByteSlowAsync(cancelToken).ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<int> ReadByteSlowAsync(CancellationToken cancelToken)
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

/// <summary>
/// Very fast parser, only supports strings and nulls
/// Never generates parsing errors
/// </summary>
internal class ReusableChunkParser : IChunkParser
{
    private readonly Stream _stream;

    internal ReusableChunkParser(Stream stream)
    {
        _stream = stream;
    }

    public Task ParseChunkAsync(IResultChunk chunk, CancellationToken cancellationToken)
    {
        var rc = (SFReusableChunk)chunk;

        var input = new FastStreamWrapper(_stream);
        var ms = new FastMemoryStream();

        return Task.Run(() => ParseChunk(rc, input, ms, cancellationToken), cancellationToken);
    }

    private static async Task ParseChunk(SFReusableChunk rc, FastStreamWrapper input, FastMemoryStream ms, CancellationToken cancelToken)
    {
        var inString = false;
        for (; ; )
        {
            var c = await input.ReadByteAsync(cancelToken).ConfigureAwait(false);

            if (c < 0)
                break;

            if (!inString)
            {
                switch (c)
                {
                    case '"': // " quote means begin string
                        inString = true;
                        break;
                    case 'n': // n means null
                        rc.AddCell(null, 0);
                        break;
                }
                // ignore anything else
            }
            else
            {
                switch (c)
                {
                    // Inside a string, look for end string
                    // Anything else is saved in the buffer
                    case '"':
                        rc.AddCell(ms.GetBuffer(), ms.Length);
                        ms.Clear();
                        inString = false;
                        break;
                    case '\\':
                    {
                        var caseU = false;
                        // Process next character
                        c = await input.ReadByteAsync(cancelToken).ConfigureAwait(false);
                        switch (c)
                        {
                            case 'n':
                                c = '\n';
                                break;
                            case 'r':
                                c = '\r';
                                break;
                            case 'b':
                                c = '\b';
                                break;
                            case 't':
                                c = '\t';
                                break;
                            case 'u':
                                caseU = true;
                                var byteStr = new StringBuilder("");
                                for (var i = 0; i < 4; i++)
                                {
                                    var readByte = await input.ReadByteAsync(cancelToken).ConfigureAwait(false);
                                    byteStr.Append((char)readByte);
                                }

                                var ascii = int.Parse(byteStr.ToString(), System.Globalization.NumberStyles.HexNumber);
                                var asciiChar = (char)ascii;
                                ms.WriteByte((byte)asciiChar);
                                break;
                            case -1:
                                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected end of stream in escape sequence");
                        }

                        // The 'u' case already writes to stream so skip to prevent re-writing
                        // If not skipped, unicode characters are added an extra u (e.g "/u007f" becomes "/u007fu")
                        if (!caseU)
                        {
                            ms.WriteByte((byte)c);
                        }

                        break;
                    }
                    default:
                        ms.WriteByte((byte)c);
                        break;
                }
            }
        }

        if (inString)
            throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected end of stream in string");
    }
}
