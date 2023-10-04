/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

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
        bool _useV2JsonParserDefault;
        int _chunkParserVersionDefault;

        [SetUp]
        public void BeforeTest()
        {
            _useV2JsonParserDefault = SFConfiguration.Instance().UseV2JsonParser;
            _chunkParserVersionDefault = SFConfiguration.Instance().ChunkParserVersion;
        }

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().UseV2JsonParser = _useV2JsonParserDefault; // Return to default version
            SFConfiguration.Instance().ChunkParserVersion = _chunkParserVersionDefault; // Return to default version
        }

        [Test]
        public void TestGetParser([Values(false, true)] bool useV2JsonParser, [Values(1, 2, 3, 4)] int chunkParserVersion)
        {
            // Set configuration settings
            SFConfiguration.Instance().UseV2JsonParser = useV2JsonParser;
            SFConfiguration.Instance().ChunkParserVersion = chunkParserVersion;

            // Get parser using sample stream
            byte[] bytes = Encoding.UTF8.GetBytes("test");
            Stream stream = new MemoryStream(bytes);

            IChunkParser parser = null;

            // GetParser() throws an error when ChunkParserVersion is not 1-3
            if (chunkParserVersion == 4 && !useV2JsonParser)
            {
                Exception ex = Assert.Throws<Exception>(() => parser = ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream));
                Assert.AreEqual("Unsupported Chunk Parser version specified in the SFConfiguration", ex.Message);
            }
            else
            {
                parser = ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream);
            }

            // GetParser() returns ChunkDeserializer when UseV2JsonParser is true
            if (SFConfiguration.Instance().UseV2JsonParser)
            {
                Assert.IsTrue(parser is ChunkDeserializer);
            }
            else
            {
                if (SFConfiguration.Instance().GetChunkParserVersion() == 1)
                {
                    Assert.IsTrue(parser is ChunkStreamingParser);
                }
                else if (SFConfiguration.Instance().GetChunkParserVersion() == 2)
                {
                    Assert.IsTrue(parser is ChunkDeserializer);
                }
                else if (SFConfiguration.Instance().GetChunkParserVersion() == 3)
                {
                    Assert.IsTrue(parser is ReusableChunkParser);
                }
            }
        }
    }
}
