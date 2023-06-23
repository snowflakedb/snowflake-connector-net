/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    [TestFixture, NonParallelizable]
    class ChunkStreamingParserTest
    {
        int ChunkParserVersionDefault = SFConfiguration.Instance().GetChunkParserVersion();

        [SetUp]
        public void BeforeTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = 1; // ChunkStreamingParser
        }

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = ChunkParserVersionDefault; // Return to default version
        }

        [Test]
        [Ignore("ChunkStreamingParserTest")]
        public void ChunkStreamingParserTestDone()
        {
            // Do nothing;
        }

        public IChunkParser getParser(string data)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            return ChunkParserFactory.Instance.GetParser(stream);
        }

        [Test]
        public async Task TestParsingEmptyChunk()
        {
            // Create sample data for parser
            string data = "[ ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[0, 0]);

            await parser.ParseChunk(chunk);

            Assert.AreEqual(0, chunk.rowSet.GetLength(0)); // Check row length
            Assert.AreEqual(0, chunk.rowSet.GetLength(0)); // Check col length
            Assert.Throws<IndexOutOfRangeException>(() => chunk.ExtractCell(0, 0).SafeToString());
        }

        [Test]
        public async Task TestParsingEmptyArraysInChunk()
        {
            // Create sample data for parser
            string data = "[ [],  [] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[2, 0]);

            await parser.ParseChunk(chunk);

            Assert.AreEqual(2, chunk.rowSet.GetLength(0)); // Check row length
            Assert.AreEqual(0, chunk.rowSet.GetLength(1)); // Check col length
            Assert.Throws<IndexOutOfRangeException>(() => chunk.ExtractCell(0, 0).SafeToString());
        }

        [Test]
        public async Task TestParsingNonJsonArrayChunk()
        {
            // Create a sample data using data not contained in an array
            string data = "[ \"1\", \"1.234\", \"abcde\" ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 3]);

            await parser.ParseChunk(chunk);

            // ChunkStreamingParser is able to parse one row that is not inside of an array
            Assert.AreEqual("1", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual("abcde", chunk.ExtractCell(0, 2).SafeToString());
        }

        [Test]
        public void TestParsingNonJsonArrayAndOneArray()
        {
            // Create a sample data using non-array data and an array
            string data = "[ \"1\", \"1.234\", \"abcde\", [\"2\", \"5.678\", \"fghi\"] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 3]);

            // Should throw an error when parsing non-array and array data together
            Assert.ThrowsAsync<IndexOutOfRangeException>(async () => await parser.ParseChunk(chunk));
        }

        [Test]
        public void TestParsingJsonObjectChunk()
        {
            // Create a sample data using JSON objects instead
            string data = "[ {\"1\", \"1.234\", \"abcde\"},  {\"2\", \"5.678\", \"fghi\"} ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[2, 3]);

            // Should throw an error when parsing a JSON object
            Assert.ThrowsAsync<SnowflakeDbException>(async () => await parser.ParseChunk(chunk));
        }

        [Test]
        public async Task TestParsingSimpleChunk()
        {
            // Create sample data for parser
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[2, 3]);

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
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[2, 3]);

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
