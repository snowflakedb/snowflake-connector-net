using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class IdleTimeoutReadStreamTest
{
    // --- Idle timeout tests ---

    [Fact]
    public async Task TestReadAsyncCompletesNormallyWhenStreamResponds()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        var buffer = new byte[5];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        Assert.Equal(5, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestReadAsyncWithZeroIdleTimeoutDisablesIdleCheck()
    {
        var data = new byte[] { 1, 2, 3 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestIdleTimeoutFiresBetweenReads()
    {
        var data = new byte[20];
        Array.Fill(data, (byte)0xAA);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        var buffer = new byte[10];
        await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        // Wait longer than idle timeout between reads
        await Task.Delay(100);

        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None));

        Assert.Contains("No data received for", ex.Message);
    }

    [Fact]
    public async Task TestIdleTimeoutResetsAfterEachSuccessfulRead()
    {
        var data = new byte[100];
        Array.Fill(data, (byte)0xAB);
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromMilliseconds(200), TimeSpan.Zero);

        var buffer = new byte[10];
        for (var i = 0; i < 10; i++)
        {
            var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);
            Assert.Equal(10, read);
        }
    }

    [Fact]
    public async Task TestCallerCancellationPropagatesWithIdleTimeout()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromHours(2), TimeSpan.Zero);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var buffer = new byte[10];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, cts.Token));
    }

    // --- Read timeout tests ---

    [Fact]
    public async Task TestReadTimeoutFiresWhenReadExceedsDeadline()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromMilliseconds(50));

        var buffer = new byte[10];
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None));

        Assert.Contains("Read took longer than", ex.Message);
        Assert.IsType<TaskCanceledException>(ex.InnerException);
    }

    [Fact]
    public async Task TestZeroReadTimeoutDisablesPerReadDeadline()
    {
        var data = new byte[] { 7, 8, 9 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestReadCompletesBeforeReadTimeoutDeadline()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromSeconds(5));

        var buffer = new byte[5];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        Assert.Equal(5, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestBothTimeoutsConfiguredReadHangsTriggersReadTimeout()
    {
        using var inner = new NeverEndingStream();
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(50));

        var buffer = new byte[10];
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None));

        Assert.Contains("Read took longer than", ex.Message);
    }

    // --- Edge cases ---

    [Fact]
    public async Task TestBothTimeoutsZeroActsAsPassthrough()
    {
        var data = new byte[] { 42, 43, 44 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.Zero);

        var buffer = new byte[3];
        var read = await stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [Fact]
    public async Task TestDisposeDoesNotThrowAfterTimeout()
    {
        using var inner = new NeverEndingStream();
        var stream = new IdleTimeoutReadStream(inner, TimeSpan.Zero, TimeSpan.FromMilliseconds(30));

        var buffer = new byte[10];
        await Assert.ThrowsAsync<TimeoutException>(
            () => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None));

        stream.Dispose();
    }

    [Fact]
    public void TestSyncReadDelegatesToInnerWithoutTimeout()
    {
        var data = new byte[] { 10, 20, 30 };
        using var inner = new MemoryStream(data);
        using var stream = new IdleTimeoutReadStream(inner, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

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
            await Task.Delay(Timeout.Infinite, cancellationToken);
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
