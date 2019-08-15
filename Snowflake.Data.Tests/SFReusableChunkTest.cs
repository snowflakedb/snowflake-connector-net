/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using System.IO;
    using System.Text;
    using Snowflake.Data.Core;

    [TestFixture]
    class SFReusableChunkTest
    {
        [Test]
        public void TestSimpleChunk()
        {
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = 100,
                rowCount = 2
            };

            SFReusableChunk chunk = new SFReusableChunk(3);
            chunk.Reset(chunkInfo, 0);

            parser.ParseChunk(chunk);

            Assert.AreEqual("1", chunk.ExtractCell(0, 0));
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1));
            Assert.AreEqual("abcde", chunk.ExtractCell(0, 2));
            Assert.AreEqual("2", chunk.ExtractCell(1, 0));
            Assert.AreEqual("5.678", chunk.ExtractCell(1, 1));
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2));
        }

        [Test]
        public void TestChunkWithNull()
        {
            string data = "[ [null, \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = 100,
                rowCount = 2
            };

            SFReusableChunk chunk = new SFReusableChunk(3);
            chunk.Reset(chunkInfo, 0);

            parser.ParseChunk(chunk);

            Assert.AreEqual(null, chunk.ExtractCell(0, 0));
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1));
            Assert.AreEqual(null, chunk.ExtractCell(0, 2));
            Assert.AreEqual("2", chunk.ExtractCell(1, 0));
            Assert.AreEqual(null, chunk.ExtractCell(1, 1));
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2));
        }
        
        [Test]
        public void TestChunkWithDate()
        {
            string data = "[ [null, \"2019-08-21T11:58:00\", null],  [\"2\", null, \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = 100,
                rowCount = 2
            };

            SFReusableChunk chunk = new SFReusableChunk(3);
            chunk.Reset(chunkInfo, 0);

            parser.ParseChunk(chunk);

            Assert.AreEqual(null, chunk.ExtractCell(0, 0));
            Assert.AreEqual("2019-08-21T11:58:00", chunk.ExtractCell(0, 1));
            Assert.AreEqual(null, chunk.ExtractCell(0, 2));
            Assert.AreEqual("2", chunk.ExtractCell(1, 0));
            Assert.AreEqual(null, chunk.ExtractCell(1, 1));
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2));

        [Test]
        public void TestChunkWithEscape()
        {
            string data = "[ [\"åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = 2
            };

            SFReusableChunk chunk = new SFReusableChunk(3);
            chunk.Reset(chunkInfo, 0);

            parser.ParseChunk(chunk);

            Assert.AreEqual("åäö\nÅÄÖ\r", chunk.ExtractCell(0, 0));
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1));
            Assert.AreEqual(null, chunk.ExtractCell(0, 2));
            Assert.AreEqual("2", chunk.ExtractCell(1, 0));
            Assert.AreEqual(null, chunk.ExtractCell(1, 1));
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2));
        }
        [Test]
        public void TestChunkWithLongString()
        {
            string longstring = new string('å', 10 * 1000 * 1000);
            string data = "[ [\"åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"" + longstring + "\"] ]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = 2
            };

            SFReusableChunk chunk = new SFReusableChunk(3);
            chunk.Reset(chunkInfo, 0);

            parser.ParseChunk(chunk);

            Assert.AreEqual("åäö\nÅÄÖ\r", chunk.ExtractCell(0, 0));
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1));
            Assert.AreEqual(null, chunk.ExtractCell(0, 2));
            Assert.AreEqual("2", chunk.ExtractCell(1, 0));
            Assert.AreEqual(null, chunk.ExtractCell(1, 1));
            Assert.AreEqual(longstring, chunk.ExtractCell(1, 2));
        }
    }
}
