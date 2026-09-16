using System;
using System.IO;

namespace Snowflake.Data.Core.FileTransfer;

/// <summary>
/// Pairs a main stream with a tightly coupled helper stream (e.g. a <see cref="System.Security.Cryptography.CryptoStream"/>)
/// so both can be disposed together once the caller is done reading the main stream.
/// Without this, disposing the helper inside the producing method would also close the main stream.
/// </summary>
internal class StreamPair : IDisposable
{
    /// <summary>The primary stream the caller reads from.</summary>
    public Stream MainStream { get; init; }

    /// <summary>A coupled stream (e.g. CryptoStream) that must stay alive until <see cref="MainStream"/> is consumed.</summary>
    public Stream HelperStream { get; init; }

    public void Dispose()
    {
        MainStream?.Dispose();
        HelperStream?.Dispose();
    }
}
