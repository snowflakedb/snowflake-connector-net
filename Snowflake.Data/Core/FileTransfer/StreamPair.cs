using System;
using System.IO;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class StreamPair: IDisposable
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
