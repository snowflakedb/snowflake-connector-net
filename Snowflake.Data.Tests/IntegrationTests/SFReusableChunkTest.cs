using System;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using NUnit.Framework;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{

    [TestFixture, NonParallelizable]
    class SFReusableChunkTest : SFBaseTest
    {
        [Test]
        public void TestDelCharPr431()
        {
            const int TEST_ROW_COUNT = 5000;
            
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                try
                {
                    SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                    CreateOrReplaceTable(conn, TableName, new[] { "col STRING" });

                    IDbCommand cmd = conn.CreateCommand();
                    int rowCount = 0;

                    // Insert data with DEL character (0x7F) embedded: "snow\x7FFLAKE"
                    string insertCommand = $"insert into {TableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {TEST_ROW_COUNT})))";
                    cmd.CommandText = insertCommand;
                    IDataReader insertReader = cmd.ExecuteReader();
                    Assert.AreEqual(TEST_ROW_COUNT, insertReader.RecordsAffected);

                    string selectCommand = $"select * from {TableName}";
                    cmd.CommandText = selectCommand;

                    rowCount = 0;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var obj = new object[reader.FieldCount];
                            reader.GetValues(obj);
                            var val = obj[0] ?? System.String.Empty;
                            // Filter out rows containing literal "u007f" or "\u007fu" strings
                            // (The actual data contains DEL character but not these literal strings)
                            if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                            {
                                rowCount++;
                            }
                        }
                    }
                    Assert.AreEqual(TEST_ROW_COUNT, rowCount, "All rows should be counted as they don't contain literal 'u007f' strings");
                }
                finally
                {
                    SessionParameterAlterer.RestoreResultFormat(conn);
                    conn.Close();
                }
            }
        }

        [Test]
        public void TestParseJson()
        {
            IChunkParserFactory previous = ChunkParserFactory.Instance;

            try
            {
                ChunkParserFactory.Instance = new TestChunkParserFactory(1);

                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.Open();

                    SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                    CreateOrReplaceTable(conn, TableName, new[] { "src VARIANT" });

                    IDbCommand cmd = conn.CreateCommand();
                    int rowCount = 0;

                    string insertCommand = $@"
-- borrowed from https://docs.snowflake.com/en/user-guide/querying-semistructured.html#sample-data-used-in-examples
insert into {TableName} (
select parse_json('{{
    ""date"" : ""2017 - 04 - 28"",
    ""dealership"" : ""Valley View Auto Sales"",
    ""salesperson"" : {{
                    ""id"": ""55"",
      ""name"": ""Frank Beasley""
    }},
    ""customer"" : [
      {{ ""name"": ""Joyce Ridgely"", ""phone"": ""16504378889"", ""address"": ""San Francisco, CA""}}
    ],
    ""vehicle"" : [
       {{ ""make"": ""Honda"", ""model"": ""Civic"", ""year"": ""2017"", ""price"": ""20275"", ""extras"":[""ext warranty"", ""paint protection""]}}
    ]
}}') from table(generator(rowcount => 500))
)
";
                    cmd.CommandText = insertCommand;
                    IDataReader insertReader = cmd.ExecuteReader();
                    Assert.AreEqual(500, insertReader.RecordsAffected);

                    string selectCommand = $"select * from {TableName}";
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

                    SessionParameterAlterer.RestoreResultFormat(conn);
                    conn.Close();
                }
            }
            finally
            {
                ChunkParserFactory.Instance = previous;
            }
        }

        [Test, NonParallelizable]
        public void TestChunkRetry()
        {
            const int RETRY_FAILURE_COUNT = 6;
            const int TEST_ROW_COUNT = 5000;

            IChunkParserFactory previous = ChunkParserFactory.Instance;
            TestChunkParserFactory testFactory = new TestChunkParserFactory(RETRY_FAILURE_COUNT);

            try
            {
                ChunkParserFactory.Instance = testFactory;

                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.Open();

                    try
                    {
                        SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                        CreateOrReplaceTable(conn, TableName, new[] { "col STRING" });

                        IDbCommand cmd = conn.CreateCommand();
                        int rowCount = 0;

                        string insertCommand = $"insert into {TableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {TEST_ROW_COUNT})))";
                        cmd.CommandText = insertCommand;
                        IDataReader insertReader = cmd.ExecuteReader();
                        Assert.AreEqual(TEST_ROW_COUNT, insertReader.RecordsAffected);

                        string selectCommand = $"select * from {TableName}";
                        cmd.CommandText = selectCommand;

                        rowCount = 0;
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var obj = new object[reader.FieldCount];
                                reader.GetValues(obj);
                                var val = obj[0] ?? System.String.Empty;
                                // Filter out rows containing literal "u007f" or "\u007fu" strings
                                if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                                {
                                    rowCount++;
                                }
                            }
                        }
                        Assert.AreEqual(TEST_ROW_COUNT, rowCount);

                        Assert.IsTrue(testFactory.ExceptionsThrown >= RETRY_FAILURE_COUNT,
                            $"Expected at least {RETRY_FAILURE_COUNT} retry attempts, but only {testFactory.ExceptionsThrown} occurred");
                    }
                    finally
                    {
                        SessionParameterAlterer.RestoreResultFormat(conn);
                        conn.Close();
                    }
                }
            }
            finally
            {
                ChunkParserFactory.Instance = previous;
            }
        }

        [Test]
        public void TestExceptionThrownWhenChunkDownloadRetryCountExceeded()
        {
            IChunkParserFactory previous = ChunkParserFactory.Instance;

            try
            {
                ChunkParserFactory.Instance = new TestChunkParserFactory(8); // larger than default max retry of 7

                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.Open();

                    CreateOrReplaceTable(conn, TableName, new[] { "col STRING" });

                    IDbCommand cmd = conn.CreateCommand();
                    int rowCount = 0;

                    int largeTableRowCount = 50000;
                    string insertCommand = $"insert into {TableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {largeTableRowCount})))";
                    cmd.CommandText = insertCommand;
                    IDataReader insertReader = cmd.ExecuteReader();
                    Assert.AreEqual(largeTableRowCount, insertReader.RecordsAffected);

                    string selectCommand = $"select * from {TableName}";
                    cmd.CommandText = selectCommand;

                    rowCount = 0;
                    Assert.Throws<AggregateException>(() =>
                    {
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
                    });
                    Assert.AreNotEqual(largeTableRowCount, rowCount);

                    conn.Close();
                }
            }
            finally
            {
                ChunkParserFactory.Instance = previous;
            }
        }

        class TestChunkParserFactory : IChunkParserFactory
        {
            private int _exceptionsThrown;
            private readonly int _expectedExceptionsNumber;

            public TestChunkParserFactory(int exceptionsToThrow)
            {
                _expectedExceptionsNumber = exceptionsToThrow;
                _exceptionsThrown = 0;
            }

            public int ExceptionsThrown => _exceptionsThrown;

            public IChunkParser GetParser(ResultFormat resultFormat, Stream stream)
            {
                if (++_exceptionsThrown <= _expectedExceptionsNumber)
                    return new ThrowingReusableChunkParser();

                return new ReusableChunkParser(stream);
            }
        }

        class ThrowingReusableChunkParser : IChunkParser
        {
            public Task ParseChunk(IResultChunk chunk)
            {
                throw new Exception("json parsing error.");
            }
        }
    }
}
