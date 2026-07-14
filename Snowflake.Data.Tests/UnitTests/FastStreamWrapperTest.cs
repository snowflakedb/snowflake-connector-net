using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

#if !NET8_0_OR_GREATER
using Snowflake.Data.Tests.IntegrationTests;
#endif

namespace Snowflake.Data.Tests.UnitTests;

public sealed class FastStreamWrapperTest
{
    [SFFact]
    public async Task TestReadByteAsyncThrowsWhenTokenIsPreCancelled()
    {
        var data = "hello"u8.ToArray();
        var stream = new MemoryStream(data);
        var wrapper = new FastStreamWrapper(stream);

        var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wrapper.ReadByteAsync(cts.Token));
    }

    [SFFact]
    public async Task TestReadByteAsyncReturnsDataWithValidToken()
    {
        var data = "AB"u8.ToArray();
        var stream = new MemoryStream(data);
        var wrapper = new FastStreamWrapper(stream);

        using var cts = new CancellationTokenSource();

        Assert.Equal('A', await wrapper.ReadByteAsync(cts.Token));
        Assert.Equal('B', await wrapper.ReadByteAsync(cts.Token));
        Assert.Equal(-1, await wrapper.ReadByteAsync(cts.Token));
    }

    [SFFact]
    public async Task TestReadByteAsyncReturnsMinusOneAtEndOfStream()
    {
        var stream = new MemoryStream([]);
        var wrapper = new FastStreamWrapper(stream);

        var result = await wrapper.ReadByteAsync(CancellationToken.None);

        Assert.Equal(-1, result);
    }

    [SFFact]
    public async Task TestReadByteAsyncThrowsWhenCancelledDuringBufferRefill()
    {
        // Create a stream larger than the internal buffer (32768 bytes)
        // so that a multiple ReadAsync calls are needed
        var data = new byte[32768*3];
        Array.Fill(data, (byte)'x');
        var cts = new CancellationTokenSource();

        var stream = new CancellingReadStream(data, cts, data.Length / 2 + 1);
        var wrapper = new FastStreamWrapper(stream);

        // First and second buffer fill should work - read all bytes from first buffer
        for (var i = 0; i < 32768*2; i++)
            Assert.Equal('x', await wrapper.ReadByteAsync(cts.Token));

        // Next read should trigger second buffer fill which cancels
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => wrapper.ReadByteAsync(cts.Token));
    }

    [SFFact]
    public async Task TestReadByteAsyncReadsEntireStreamWithCancellationTokenNone()
    {
        var data = "test data 123"u8.ToArray();
        var stream = new MemoryStream(data);
        var wrapper = new FastStreamWrapper(stream);

        var result = new StringBuilder();
        int b;
        while ((b = await wrapper.ReadByteAsync(CancellationToken.None)) >= 0)
            result.Append((char)b);

        Assert.Equal("test data 123", result.ToString());
    }
}
