namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security;
    using System.Text;
    using System.Threading;

    [TestFixture, NonParallelizable]
    class ChunkParserFactoryTest
    {
        int ChunkParserVersionDefault = SFConfiguration.Instance().GetChunkParserVersion();

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = ChunkParserVersionDefault; // Return to default version
        }

        [Test]
        public void TestGetParser([Values(1, 2, 3, 4)] int chunkParserVersion)
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
                        Assert.That(ex.Message, Does.Contain("Unsupported Chunk Parser version"));
                    }
                    else
                    {
                        parser = ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream);
                        Assert.IsTrue(parser is ReusableChunkParser);
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
