/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    [TestFixture, NonParallelizable]
    class ChunkDeserializerTest
    {
        private int _chunkParserVersionDefault;

        [SetUp]
        public void BeforeTest()
        {
            _chunkParserVersionDefault = SFConfiguration.Instance().ChunkParserVersion;
            SFConfiguration.Instance().ChunkParserVersion = 2; // ChunkDeserializer
        }

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().ChunkParserVersion = _chunkParserVersionDefault; // Return to default version
        }

        public IChunkParser getParser(string data)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            return ChunkParserFactory.Instance.GetParser(ResultFormat.JSON, stream);
        }

        [Test]
        public async Task TestParsingEmptyChunk()
        {
            // Create sample data for parser
            string data = "[ ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);
            chunk.Next();
            
            Assert.AreEqual(0, chunk.RowSet.GetLength(0)); // Check row length
            Assert.AreEqual(0, chunk.RowSet.GetLength(1)); // Check col length
            Assert.Throws<IndexOutOfRangeException>(() => chunk.ExtractCell(0).SafeToString());
        }

        [Test]
        public async Task TestParsingEmptyArraysInChunk()
        {
            // Create sample data for parser
            string data = "[ [],  [] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);
            chunk.Next();
            
            Assert.AreEqual(2, chunk.RowSet.GetLength(0)); // Check row length
            Assert.AreEqual(0, chunk.RowSet.GetLength(1)); // Check col length
            Assert.Throws<IndexOutOfRangeException>(() => chunk.ExtractCell(0).SafeToString());
        }

        [Test]
        public void TestParsingNonJsonChunk()
        {
            // Create a sample data using non-JSON instead
            string data = "[ \"1\", \"1.234\", \"abcde\" ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            // Should throw an error when parsing non-JSONArray
            Assert.ThrowsAsync<JsonSerializationException>(async () => await parser.ParseChunk(chunk));
        }

        [Test]
        public void TestParsingNonJsonArrayChunk()
        {
            // Create a sample data using JSON objects instead
            string data = "[ {\"1\", \"1.234\", \"abcde\"},  {\"2\", \"5.678\", \"fghi\"} ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            // Should throw an error when parsing non-JSONArray
            Assert.ThrowsAsync<JsonSerializationException>(async () => await parser.ParseChunk(chunk));
        }

        [Test]
        public async Task TestParsingSimpleChunk()
        {
            // Create sample data for parser
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);
            
            chunk.Next();
            Assert.AreEqual("1", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("abcde", chunk.ExtractCell(2).SafeToString());
            
            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("5.678", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [Test]
        public async Task TestParsingChunkWithNullValue()
        {
            // Create sample data that contain null values
            string data = "[ [null, \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            IChunkParser parser = getParser(data);

            SFResultChunk chunk = new SFResultChunk(new string[1, 1]);

            await parser.ParseChunk(chunk);

            chunk.Next();
            Assert.AreEqual(null, chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(2).SafeToString());
            
            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(2).SafeToString());
        }
    }
}
