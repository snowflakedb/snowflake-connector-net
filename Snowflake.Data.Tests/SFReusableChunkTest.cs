/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
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
        [Ignore("ReusableChunkTest")]
        public void ReusableChunkTestDone()
        {
            // Do nothing;
        }

        [Test]
        public async Task TestSimpleChunk()
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

            await parser.ParseChunk(chunk);

            Assert.AreEqual("1", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual("abcde", chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual("5.678", chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestChunkWithNull()
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

            await parser.ParseChunk(chunk);

            Assert.AreEqual(null, chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestChunkWithDate()
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

            await parser.ParseChunk(chunk);

            Assert.AreEqual(null, chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("2019-08-21T11:58:00", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestChunkWithEscape()
        {
            string data = "[ [\"\\\\åäö\\nÅÄÖ\\r\", \"1.234\", null],  [\"2\", null, \"fghi\"] ]";
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

            await parser.ParseChunk(chunk);

            Assert.AreEqual("\\åäö\nÅÄÖ\r", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual("fghi", chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestChunkWithLongString()
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

            await parser.ParseChunk(chunk);

            Assert.AreEqual("åäö\nÅÄÖ\r", chunk.ExtractCell(0, 0).SafeToString());
            Assert.AreEqual("1.234", chunk.ExtractCell(0, 1).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(0, 2).SafeToString());
            Assert.AreEqual("2", chunk.ExtractCell(1, 0).SafeToString());
            Assert.AreEqual(null, chunk.ExtractCell(1, 1).SafeToString());
            Assert.AreEqual(longstring, chunk.ExtractCell(1, 2).SafeToString());
        }

        [Test]
        public async Task TestParserError1()
        {
            // Unterminated escape sequence
            string data = "[ [\"åäö\\";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = 1
            };

            SFReusableChunk chunk = new SFReusableChunk(1);
            chunk.Reset(chunkInfo, 0);

            try
            {
                await parser.ParseChunk(chunk);
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
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = 1
            };

            SFReusableChunk chunk = new SFReusableChunk(1);
            chunk.Reset(chunkInfo, 0);

            try
            {
                await parser.ParseChunk(chunk);
                Assert.Fail();
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public async Task TestParserWithTab()
        {
            // Unterminated string
            string data = "[[\"abc\t\"]]";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);
            IChunkParser parser = new ReusableChunkParser(stream);

            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "fake",
                uncompressedSize = bytes.Length,
                rowCount = 1
            };

            SFReusableChunk chunk = new SFReusableChunk(1);
            chunk.Reset(chunkInfo, 0);

            await parser.ParseChunk(chunk);
            string val = chunk.ExtractCell(0, 0).SafeToString();
            Assert.AreEqual("abc\t", chunk.ExtractCell(0, 0).SafeToString());
        }

    }
    [TestFixture]
    class SFReusableChunkTest2 : SFBaseTest
    {
        [Test]
        [Ignore("ReusableChunkTest2")]
        public void ReusableChunkTest2Done()
        {
            // Do nothing;
        }

        [Test]
        public void testDelCharPr431()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                int rowCount = 0;

                string createCommand = "create or replace table del_test (col string)";
                cmd.CommandText = createCommand;
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                int largeTableRowCount = 100000;
                string insertCommand = $"insert into del_test(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {largeTableRowCount})))";
                cmd.CommandText = insertCommand;
                IDataReader insertReader = cmd.ExecuteReader();
                Assert.AreEqual(largeTableRowCount, insertReader.RecordsAffected);

                string selectCommand = "select * from del_test";
                cmd.CommandText = selectCommand;

                rowCount = 0;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var obj = new object[reader.FieldCount];
                        reader.GetValues(obj);
                        var val = obj[0] ?? System.String.Empty;
                        if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                        {
                            rowCount++;
                        }
                    }
                }
                Assert.AreEqual(largeTableRowCount, rowCount);

                cmd.CommandText = "drop table if exists del_test";
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
            }
        }

        [Test]
        public void testParseJson()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "; FORCEPARSEERROR = true";
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                int rowCount = 0;

                string createCommand = "create or replace table car_sales(src variant)";
                cmd.CommandText = createCommand;
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                string insertCommand = @"
-- borrowed from https://docs.snowflake.com/en/user-guide/querying-semistructured.html#sample-data-used-in-examples
insert into car_sales (
select parse_json('{ 
    ""date"" : ""2017 - 04 - 28"", 
    ""dealership"" : ""Valley View Auto Sales"",
    ""salesperson"" : {
                    ""id"": ""55"",
      ""name"": ""Frank Beasley""
    },
    ""customer"" : [
      { ""name"": ""Joyce Ridgely"", ""phone"": ""16504378889"", ""address"": ""San Francisco, CA""}
    ],
    ""vehicle"" : [
       { ""make"": ""Honda"", ""model"": ""Civic"", ""year"": ""2017"", ""price"": ""20275"", ""extras"":[""ext warranty"", ""paint protection""]}
    ]
}') from table(generator(rowcount => 500))
)
";
                cmd.CommandText = insertCommand;
                IDataReader insertReader = cmd.ExecuteReader();
                Assert.AreEqual(500, insertReader.RecordsAffected);

                string selectCommand = "select * from car_sales";
                cmd.CommandText = selectCommand;
                cmd.CommandType = System.Data.CommandType.Text;

                rowCount = 0;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Newtonsoft.Json.JsonConvert.DeserializeObject(reader[0].ToString());
                        rowCount++;
                    }
                }
                Assert.AreEqual(500, rowCount);

                cmd.CommandText = "drop table if exists car_sales";
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
            }
        }

        [Test]
        public void testChunkRetry()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "FORCEPARSEERROR=true";
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                int rowCount = 0;

                string createCommand = "create or replace table del_test (col string)";
                cmd.CommandText = createCommand;
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                int largeTableRowCount = 100000;
                string insertCommand = $"insert into del_test(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {largeTableRowCount})))";
                cmd.CommandText = insertCommand;
                IDataReader insertReader = cmd.ExecuteReader();
                Assert.AreEqual(largeTableRowCount, insertReader.RecordsAffected);

                string selectCommand = "select * from del_test";
                cmd.CommandText = selectCommand;

                rowCount = 0;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var obj = new object[reader.FieldCount];
                        reader.GetValues(obj);
                        var val = obj[0] ?? System.String.Empty;
                        if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                        {
                            rowCount++;
                        }
                    }
                }
                Assert.AreEqual(largeTableRowCount, rowCount);

                cmd.CommandText = "drop table if exists del_test";
                rowCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, rowCount);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
            }
        }

    }
}
