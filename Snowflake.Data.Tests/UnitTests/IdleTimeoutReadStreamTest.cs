using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Snowflake.Data.Tests.Util.Shims;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class IdleTimeoutReadStreamTest
{
    [SFFact]
    public async Task TestReadAsyncCompletesNormallyWhenStreamResponds()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        var buffer = new byte[5];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(5, read);
        Assert.Equal(data, buffer);
    }

    [SFFact]
    public async Task TestReadAsyncWithZeroIdleTimeoutDisablesIdleCheck()
    {
        var data = new byte[] { 1, 2, 3 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [SFFact]
    public async Task TestIdleTimeoutFiresBetweenReads()
    {
        var data = new byte[20];
        data.Fill((byte)0xAA);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        var buffer = new byte[10];
        _ = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        // Wait longer than idle timeout between reads
        await Task.Delay(100).ConfigureAwait(false);

        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(false);

        Assert.Contains("No data received for", ex.Message);
    }

    [SFFact]
    public async Task TestIdleTimeoutResetsAfterEachSuccessfulRead()
    {
        var data = new byte[100 * 100];
        data.Fill((byte)0xAB);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(200), TimeSpan.Zero);

        var buffer = new byte[100];
        for (var i = 0; i < 100; i++)
        {
            var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);
            await Task.Delay(10).ConfigureAwait(false);
            Assert.Equal(100, read);
        }
    }

    [SFFact]
    public async Task TestCallerCancellationPropagatesWithIdleTimeout()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromHours(2), TimeSpan.Zero);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var buffer = new byte[10];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, cts.Token)).ConfigureAwait(false);
    }

    [SFFact]
    public async Task TestReadTimeoutFiresWhenReadExceedsDeadline()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromMilliseconds(50));

        var buffer = new byte[10];
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(false);

        Assert.Contains("Read took longer than", ex.Message);
        Assert.IsType<TaskCanceledException>(ex.InnerException);
    }

    [SFFact]
    public async Task TestReadCompletesBeforeReadTimeoutDeadline()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromSeconds(5));

        var buffer = new byte[5];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(5, read);
        Assert.Equal(data, buffer);
    }

    [SFFact]
    public async Task TestBothTimeoutsConfiguredReadHangsTriggersReadTimeout()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(50));

        var buffer = new byte[10];
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(false);

        Assert.Contains("Read took longer than", ex.Message);
    }

    [SFFact]
    public async Task TestBothTimeoutsZeroActsAsPassthrough()
    {
        var data = new byte[] { 42, 43, 44 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [SFFact]
    public async Task TestDisposeDoesNotThrowAfterTimeout()
    {
        using var inner = new NeverEndingStream();
        var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromMilliseconds(30));

        var buffer = new byte[10];
        await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(false);

        stream.Dispose();
    }

    [SFTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestDisposeDoesNotThrowWithTimeoutsDisabled(bool disposeAfterRead)
    {
        var data = new byte[] { 1, 2, 3 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        if (disposeAfterRead)
            _ = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None).ConfigureAwait(false);

        stream.Dispose();
    }

    [SFFact]
    public void TestSyncReadDelegatesToInnerWithoutTimeout()
    {
        var data = new byte[] { 10, 20, 30 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromTicks(5), TimeSpan.FromTicks(5));

        var buffer = new byte[3];
        var read = stream.Read(buffer, 0, buffer.Length);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
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
