using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class IdleTimeoutReadStreamTest
{
    [Fact]
    public async Task TestReadAsyncCompletesNormallyWhenStreamResponds()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5));

        var buffer = new byte[5];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(5, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestReadAsyncThrowsTimeoutExceptionOnIdleTimeout()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(50));

        var buffer = new byte[10];
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(false);

        Assert.IsType<TaskCanceledException>(ex.InnerException);
    }

    [Fact]
    public async Task TestReadAsyncPropagatesCallerCancellation()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromHours(2));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var buffer = new byte[10];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, cts.Token)).ConfigureAwait(false);
    }

    [Fact]
    public async Task TestReadAsyncResetsTimeoutBetweenReads()
    {
        var data = new byte[100];
        Array.Fill(data, (byte)0xAB);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(200));

        var buffer = new byte[10];
        for (var i = 0; i < 10; i++)
        {
            var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(10, read);
        }
    }

    [Fact]
    public async Task TestReadAsyncCheckIdleTimeBetweenReads()
    {
        var data = new byte[100];
        Array.Fill(data, (byte)0xAB);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(200));

        var buffer = new byte[10];
        for (var i = 0; i < 10; i++)
        {
            var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(10, read);
        }
    }

    [Fact]
    public async Task TestReadAsyncWithZeroTimeoutDisablesIdleCheck()
    {
        var data = new byte[] { 1, 2, 3 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public void TestSyncReadDelegatesToInnerStream()
    {
        var data = new byte[] { 10, 20, 30 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5));

        var buffer = new byte[3];
        var read = stream.Read(buffer, 0, buffer.Length);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestDisposeDoesNotThrow()
    {
        var data = new byte[] { 1 };
        var inner = new MemoryStream(data);
        var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5));

        var buffer = new byte[1];
        await stream.ReadAsync(buffer, 0, 1, CancellationToken.None).ConfigureAwait(false);

        stream.Dispose();
    }

    /// <summary>
    /// A stream whose ReadAsync never completes, simulating a hung connection.
    /// </summary>
    private sealed class NeverEndingStream : Stream
    {
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            throw new NotSupportedException("This should be unreachable.");
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
