using System;
using System.IO;

namespace Snowflake.Data.Core.FileTransfer
{
    /*
     * StreamPair class has been introduced to solve the issue for a stream which is meant to be returned from a method,
     * but another helper stream is created in this method and is tightly coupled with the main stream,
     * so the helper stream cannot be closed in this method because it would close the main stream as well
     * (if CryptoStream in EncryptionProvider class would be disposed it would close the base stream as well).
     * The solution is to return both streams and dispose both of them together when processing of the main stream is over.
     */
    internal class StreamPair : IDisposable
    {
        public Stream MainStream { get; set; }
        public Stream HelperStream { get; set; }

        public void Dispose()
        {
            MainStream?.Dispose();
            HelperStream?.Dispose();
        }
    }
}
