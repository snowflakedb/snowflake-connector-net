﻿/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    [TestFixture]
    class ChunkDeserializerTest
    {
        int ChunkParserVersionDefault = SFConfiguration.Instance().GetChunkParserVersion();

        [SetUp]
        public void BeforeTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = 2; // ChunkDeserializer
        }

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = ChunkParserVersionDefault; // Return to default version
        }

        [Test]
        [Ignore("ChunkDeserializerTest")]
        public void ChunkDeserializerTestDone()
        {
            // Do nothing;
        }

        [Test]
        public async Task TestParsingSimpleChunk()
        {
            // Create sample data for parser
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = ChunkParserFactory.Instance.GetParser(stream);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);

            Assert.AreEqual("1", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual("abcde", chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual("5.678", chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestParsingChunkWithNullValue()
        {
            // Create sample data that contain null values
            string data = "[ [null, \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = ChunkParserFactory.Instance.GetParser(stream);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);

            Assert.AreEqual(null, chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }
    }
}
