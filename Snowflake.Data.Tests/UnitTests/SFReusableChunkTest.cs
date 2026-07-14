using System;
using System.Threading;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using System.Data;
    using System.IO;
    using System.Text;
    using Snowflake.Data.Core;
    using Snowflake.Data.Client;
    using System.Threading.Tasks;
    public class SFReusableChunkTest
    {
        [SFFact]
        public void TestExtractCellWithRowParameterReadsAllRows()
        {
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

#pragma warning disable CS0618 // Type or member is obsolete
            Assert.Equal("1", chunk.ExtractCell(0, 0).SafeToString());
            Assert.Equal("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.Equal("abcde", chunk.ExtractCell(0, 2).SafeToString());

            Assert.Equal("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.Equal("5.678", chunk.ExtractCell(1, 1).SafeToString());
            Assert.Equal("fghi", chunk.ExtractCell(1, 2).SafeToString());
#pragma warning restore CS0618 // Type or member is obsolete
        }

        [SFFact]
        public void TestSimpleChunk()
        {
            string data = "[ [\"1\", \"1.234\", \"abcde\"],  [\"2\", \"5.678\", \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.Equal("1", chunk.ExtractCell(0).SafeToString());
            Assert.Equal("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.Equal("abcde", chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.Equal("2", chunk.ExtractCell(0).SafeToString());
            Assert.Equal("5.678", chunk.ExtractCell(1).SafeToString());
            Assert.Equal("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [SFFact]
        public void TestChunkWithNull()
        {
            string data = "[ [null, \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.Null(chunk.ExtractCell(0).SafeToString());
            Assert.Equal("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.Null(chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.Equal("2", chunk.ExtractCell(0).SafeToString());
            Assert.Null(chunk.ExtractCell(1).SafeToString());
            Assert.Equal("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [SFFact]
        public void TestChunkWithDate()
        {
            string data = "[ [null, \"2019-08-21T11:58:00\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.Null(chunk.ExtractCell(0).SafeToString());
            Assert.Equal("2019-08-21T11:58:00", chunk.ExtractCell(1).SafeToString());
            Assert.Null(chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.Equal("2", chunk.ExtractCell(0).SafeToString());
            Assert.Null(chunk.ExtractCell(1).SafeToString());
            Assert.Equal("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [SFFact]
        public void TestChunkWithEscape()
        {
            string data = "[ [\"\\\\åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.Equal("\\åäö\nÅÄÖ\r", chunk.ExtractCell(0).SafeToString());
            Assert.Equal("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.Null(chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.Equal("2", chunk.ExtractCell(0).SafeToString());
            Assert.Null(chunk.ExtractCell(1).SafeToString());
            Assert.Equal("fghi", chunk.ExtractCell(2).SafeToString());
        }

        [SFFact]
        public void TestChunkWithLongString()
        {
            string longstring = new string('å', 10 * 1000 * 1000);
            string data = "[ [\"åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"" + longstring + "\"] ]";
            var chunk = PrepareChunkAsync(data, 3, 2).Result;

            chunk.Next();
            Assert.Equal("åäö\nÅÄÖ\r", chunk.ExtractCell(0).SafeToString());
            Assert.Equal("1.234", chunk.ExtractCell(1).SafeToString());
            Assert.Null(chunk.ExtractCell(2).SafeToString());

            chunk.Next();
            Assert.Equal("2", chunk.ExtractCell(0).SafeToString());
            Assert.Null(chunk.ExtractCell(1).SafeToString());
            Assert.Equal(longstring, chunk.ExtractCell(2).SafeToString());
        }

        [SFFact]
        public async Task TestParserError1()
        {
            // Unterminated escape sequence
            string data = "[ [\"åäö\\";

            try
            {
                await PrepareChunkAsync(data, 1, 1).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (SnowflakeDbException e)
            {
                Assert.Equal(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [SFFact]
        public async Task TestParserError2()
        {
            // Unterminated string
            string data = "[ [\"åäö";

            try
            {
                await PrepareChunkAsync(data, 1, 1).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (SnowflakeDbException e)
            {
                Assert.Equal(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [SFFact]
        public void TestParserWithTab()
        {
            // Unterminated string
            string data = "[[\"abc\t\"]]";
            var chunk = PrepareChunkAsync(data, 1, 1).Result;

            chunk.Next();
            string val = chunk.ExtractCell(0).SafeToString();
            Assert.Equal("abc\t", chunk.ExtractCell(0).SafeToString());
        }

        [SFFact]
        public void TestNextIteratesThroughAllRecords()
        {
            const int RowCount = 3;
            string data = "[ [\"1\"],  [\"2\"],  [\"3\"] ]";
            var chunk = PrepareChunkAsync(data, 1, RowCount).Result;

            for (var i = 0; i < RowCount; ++i)
            {
                Assert.True(chunk.Next());
            }
            Assert.False(chunk.Next());
        }

        [SFFact]
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
                Assert.True(chunk.Rewind());
            }
            Assert.False(chunk.Rewind());
        }

        [SFFact]
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

            Assert.Equal(0, chunk.ChunkIndex);
            Assert.Equal(chunkInfo.url, chunk.Url);
            Assert.Equal(chunkInfo.rowCount, chunk.RowCount);
            Assert.Equal(chunkInfo.uncompressedSize, chunk.UncompressedSize);
            Assert.True(chunk.data.blockCount > 0);
            Assert.True(chunk.data.metaBlockCount > 0);
        }

        [SFFact]
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

            Assert.Equal(0, chunk.ChunkIndex);
            Assert.Null(chunk.Url);
            Assert.Equal(0, chunk.RowCount);
            Assert.Equal(0, chunk.UncompressedSize);
            Assert.Equal(0, chunk.data.blockCount);
            Assert.Equal(0, chunk.data.metaBlockCount);
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

            await parser.ParseChunkAsync(chunk, CancellationToken.None).ConfigureAwait(false);
            return chunk;
        }
    }
}
