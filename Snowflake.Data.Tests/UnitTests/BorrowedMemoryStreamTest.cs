using System.IO;
using Moq;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class BorrowedMemoryStreamTest
{
    [SFFact]
    public void TestOwnedStreamIsDisposedOnDispose()
    {
        var inner = new Mock<Stream>();

        using (new BorrowedMemoryStream(inner.Object, owned: true)) { }

        inner.Verify(s => s.Close(), Times.Once);
    }

    [SFFact]
    public void TestBorrowedStreamIsNotDisposedOnDispose()
    {
        var inner = new Mock<Stream>();

        using (new BorrowedMemoryStream(inner.Object, owned: false)) { }

        inner.Verify(s => s.Close(), Times.Never);
    }

    [SFFact]
    public void TestOwnedPropertyReflectsConstructorArgument()
    {
        using var owned = new BorrowedMemoryStream(Stream.Null, owned: true);
        using var borrowed = new BorrowedMemoryStream(Stream.Null, owned: false);

        Assert.True(owned.Owned);
        Assert.False(borrowed.Owned);
    }

    [SFFact]
    public void TestReadDelegatesToInnerStream()
    {
        var data = new byte[] { 1, 2, 3 };
        using var inner = new MemoryStream(data);
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        var buffer = new byte[3];
        var read = stream.Read(buffer, 0, 3);

        Assert.Equal(3, read);
        Assert.Equal(data, buffer);
    }

    [SFFact]
    public void TestWriteDelegatesToInnerStream()
    {
        using var inner = new MemoryStream();
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        stream.Write(new byte[] { 10, 20 }, 0, 2);
        stream.Flush();

        Assert.Equal(new byte[] { 10, 20 }, inner.ToArray());
    }

    [SFFact]
    public void TestSeekDelegatesToInnerStream()
    {
        using var inner = new MemoryStream(new byte[10]);
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        var pos = stream.Seek(5, SeekOrigin.Begin);

        Assert.Equal(5, pos);
        Assert.Equal(5, stream.Position);
    }

    [SFFact]
    public void TestPositionGetSetDelegatesToInnerStream()
    {
        using var inner = new MemoryStream(new byte[10]);
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        stream.Position = 7;

        Assert.Equal(7, stream.Position);
        Assert.Equal(7, inner.Position);
    }

    [SFFact]
    public void TestLengthDelegatesToInnerStream()
    {
        using var inner = new MemoryStream(new byte[42]);
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        Assert.Equal(42, stream.Length);
    }

    [SFFact]
    public void TestCapabilitiesDelegateToInnerStream()
    {
        using var inner = new MemoryStream();
        using var stream = new BorrowedMemoryStream(inner, owned: true);

        Assert.Equal(inner.CanRead, stream.CanRead);
        Assert.Equal(inner.CanSeek, stream.CanSeek);
        Assert.Equal(inner.CanWrite, stream.CanWrite);
    }

    [SFFact]
    public void TestGetBorrowedStreamExtensionCreatesWrapper()
    {
        using var inner = new MemoryStream();
        using var borrowed = inner.GetBorrowedStream(owned: false);

        Assert.IsType<BorrowedMemoryStream>(borrowed);
        Assert.False(borrowed.Owned);
    }
}
