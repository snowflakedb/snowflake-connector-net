using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security;
    using System.Text;
    using System.Threading;
    sealed class ChunkParserFactoryTest : IDisposable
    {
        int ChunkParserVersionDefault = SFConfiguration.Instance().GetChunkParserVersion();

        public void Dispose()
        {
            SFConfiguration.Instance().ChunkParserVersion = ChunkParserVersionDefault; // Return to default version
        }

        [SFTheory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void TestGetParser(int chunkParserVersion)
        {
            try
            {
                SFConfiguration.Instance().ChunkParserVersion = chunkParserVersion;

                // Get parser using sample stream with proper disposal
                byte[] bytes = Encoding.UTF8.GetBytes("test");
                using (Stream stream = new MemoryStream(bytes))
                {
                    IChunkParser parser = null;

                    // GetParser() throws an error when ChunkParserVersion is not 1-3
                    if (chunkParserVersion == 4)
                    {
                        Exception ex = Assert.Throws<Exception>(() => parser = ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream));
                        Assert.Contains("Unsupported Chunk Parser version", ex.Message);
                    }
                    else
                    {
                        parser = ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream);
                        Assert.True(parser is ReusableChunkParser);
                    }
                }
            }
            finally
            {
                SFConfiguration.Instance().ChunkParserVersion = ChunkParserVersionDefault;
            }
        }
    }
}
