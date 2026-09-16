using System.IO;

namespace Snowflake.Data.Core.FileTransfer;

/// <summary>
/// A stream wrapper that conditionally owns the underlying stream's lifetime.
/// When <see cref="Owned"/> is <c>true</c>, disposing this wrapper also disposes
/// the inner stream. When <c>false</c>, the inner stream is left open for the
/// original owner to dispose.
/// This is used in file transfer to distinguish driver-created streams (owned)
/// from user-supplied streams (borrowed).
/// </summary>
internal sealed class BorrowedMemoryStream : Stream
{
    private readonly Stream _streamImplementation;

    /// <param name="streamImplementation">The underlying stream to wrap.</param>
    /// <param name="owned">
    /// <c>true</c> if this wrapper should dispose the inner stream on disposal;
    /// <c>false</c> to leave the inner stream open.
    /// </param>
    public BorrowedMemoryStream(Stream streamImplementation, bool owned)
    {
        _streamImplementation = streamImplementation;
        Owned = owned;
    }

    public override void Flush() => _streamImplementation.Flush();

    public override int Read(byte[] buffer, int offset, int count) => _streamImplementation.Read(buffer, offset, count);

    public override long Seek(long offset, SeekOrigin origin) => _streamImplementation.Seek(offset, origin);

    public override void SetLength(long value) => _streamImplementation.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count) => _streamImplementation.Write(buffer, offset, count);

    public override bool CanRead => _streamImplementation.CanRead;

    public override bool CanSeek => _streamImplementation.CanSeek;

    public override bool CanWrite => _streamImplementation.CanWrite;

    public override long Length => _streamImplementation.Length;

    public override long Position
    {
        get => _streamImplementation.Position;
        set => _streamImplementation.Position = value;
    }

    /// <summary>
    /// Indicates whether this wrapper owns the inner stream's lifetime.
    /// </summary>
    public bool Owned { get; }

    protected override void Dispose(bool disposing)
    {
        if (Owned)
            _streamImplementation.Dispose();
    }
}

/// <summary>
/// Extension methods for creating <see cref="BorrowedMemoryStream"/> wrappers.
/// </summary>
internal static class BorrowedStreamExtensions
{
    /// <summary>
    /// Wraps <paramref name="stream"/> in a <see cref="BorrowedMemoryStream"/> with the given ownership.
    /// </summary>
    internal static BorrowedMemoryStream GetBorrowedStream(this Stream stream, bool owned) => new(stream, owned);
}
