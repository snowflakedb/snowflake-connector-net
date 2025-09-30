using System;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using System.Data;
    using System.IO;
    using System.Text;
    using Snowflake.Data.Core;
    using Snowflake.Data.Client;
    using System.Threading.Tasks;

    [TestFixture]
    class SFReusableChunkTest
    {
        [Test]
        public void TestExtractCellWithRowParameterReadsAllRows()
        {
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

#pragma warning disable CS0618 // Type or member is obsolete
            Assert.AreEqual("1", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual("abcde", chunk.ExtractCell(0, 2).SafeToString());

            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual("5.678", chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
#pragma warning restore CS0618 // Type or member is obsolete
        }

        [Test]
        public void TestSimpleChunk()
        {
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

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
        public void TestChunkWithNull()
        {
            string data = "[ [null, \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.AreEqual(null, chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [Test]
        public void TestChunkWithDate()
        {
            string data = "[ [null, \"2019-08-21T11:58:00\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.AreEqual(null, chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("2019-08-21T11:58:00", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [Test]
        public void TestChunkWithEscape()
        {
            string data = "[ [\"\\\\åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.AreEqual("\\åäö\nÅÄÖ\r", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [Test]
        public void TestChunkWithLongString()
        {
            string longstring = new string('å', 10 * 1000 * 1000);
            string data = "[ [\"åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"" + longstring + "\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.AreEqual("åäö\nÅÄÖ\r", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.AreEqual("2", chunk.ExtractCell(0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1).SafeToString());
            Assert.AreEqual(longstring, chunk.ExtractCell(2).SafeToString());
        }

        [Test]
        public async Task TestParserError1()
        {
            // Unterminated escape sequence
            string data = "[ [\"åäö\\";

            try
            {
                await PrepareChunkAsync(data, 1, 1);
                Assert.Fail();
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public async Task TestParserError2()
        {
            // Unterminated string
            string data = "[ [\"åäö";

            try
            {
                await PrepareChunkAsync(data, 1, 1);
                Assert.Fail();
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public void TestParserWithTab()
        {
            // Unterminated string
            string data = "[[\"abc\t\"]]";
            var chunk = PrepareChunkAsync(data, 1, 1).Result;

            chunk.Next();
            string val = chunk.ExtractCell(0).SafeToString();
            Assert.AreEqual("abc\t", chunk.ExtractCell(0).SafeToString());
        }

        [Test]
        public void TestNextIteratesThroughAllRecords()
        {
            const int RowCount = 3;
            string data = "[ [\"1\"],  [\"2\"],  [\"3\"] ]";
            var chunk = PrepareChunkAsync(data, 1, RowCount).Result;

            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(chunk.Next());
            }
            Assert.IsFalse(chunk.Next());
        }

        [Test]
        public void TestRewindIteratesThroughAllRecords()
        {
            const int RowCount = 3;
            string data = "[ [\"1\"],  [\"2\"],  [\"3\"] ]";
            var chunk = PrepareChunkAsync(data, 1, RowCount).Result;

            for (var i = 0; i < RowCount; ++i)
            {
                chunk.Next();
            }
            chunk.Next();

            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(chunk.Rewind());
            }
            Assert.IsFalse(chunk.Rewind());
        }

        [Test]
        public void TestResetClearsChunkData()
        {
            const int RowCount = 3;
            string data = "[ [\"1\"],  [\"2\"],  [\"3\"] ]";
            var chunk = PrepareChunkAsync(data, 1, RowCount).Result;

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "new_url",
                uncompressedSize = 100,
                rowCount = 200
            };

            chunk.Reset(chunkInfo, 0);

            Assert.AreEqual(0, chunk.ChunkIndex);
            Assert.AreEqual(chunkInfo.url, chunk.Url);
            Assert.AreEqual(chunkInfo.rowCount, chunk.RowCount);
            Assert.AreEqual(chunkInfo.uncompressedSize, chunk.UncompressedSize);
            Assert.Greater(chunk.data.blockCount, 0);
            Assert.Greater(chunk.data.metaBlockCount, 0);
        }

        [Test]
        public void TestClearRemovesAllChunkData()
        {
            const int RowCount = 3;
            string data = "[ [\"1\"],  [\"2\"],  [\"3\"] ]";
            var chunk = PrepareChunkAsync(data, 1, RowCount).Result;

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "new_url",
                uncompressedSize = 100,
                rowCount = 200
            };

            chunk.Clear();

            Assert.AreEqual(0, chunk.ChunkIndex);
            Assert.AreEqual(null, chunk.Url);
            Assert.AreEqual(0, chunk.RowCount);
            Assert.AreEqual(0, chunk.UncompressedSize);
            Assert.AreEqual(0, chunk.data.blockCount);
            Assert.AreEqual(0, chunk.data.metaBlockCount);
        }

        private async Task<SFReusableChunk> PrepareChunkAsync(string stringData, int colCount, int rowCount)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(stringData);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = rowCount
            };

            SFReusableChunk chunk = new SFReusableChunk(colCount);
            chunk.Reset(chunkInfo, 0);

            await parser.ParseChunk(chunk);
            return chunk;
        }
    }
}
