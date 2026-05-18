using System;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using Xunit;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class SFReusableChunkTestAsync : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFReusableChunkTestAsync(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact]
        public async Task TestDelCharPr431()
        {
            const int TestRowCount = 10000;
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                try
                {
                    SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                    _fixture.CreateOrReplaceTable(conn, tableName, new[] { "col STRING" });

                    IDbCommand cmd = conn.CreateCommand();
                    var rowCount = 0;

                    // Insert data with DEL character (0x7F) embedded: "snow\x7FFLAKE"
                    var insertCommand = $"insert into {tableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {TestRowCount})))";
                    cmd.CommandText = insertCommand;
                    IDataReader insertReader = cmd.ExecuteReader();
                    Assert.Equal(TestRowCount, insertReader.RecordsAffected);

                    var selectCommand = $"select * from {tableName}";
                    cmd.CommandText = selectCommand;

                    rowCount = 0;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var obj = new object[reader.FieldCount];
                            reader.GetValues(obj);
                            var val = obj[0] ?? System.String.Empty;
                            // Count rows that don't contain literal "u007f" or "\u007fu" strings
                            // (The actual data contains DEL character but not these literal strings)
                            if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                            {
                                rowCount++;
                            }
                        }
                    }
                    Assert.Equal(TestRowCount, rowCount);
                }
                finally
                {
                    SessionParameterAlterer.RestoreResultFormat(conn);
                    await conn.CloseAsync(CancellationToken.None);
                }
            }
        }

        [SFFact]
        public async Task TestParseJson()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            var previous = ChunkParserFactory.Instance;

            try
            {
                ChunkParserFactory.Instance = new TestChunkParserFactory(1);

                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
                    await conn.OpenAsync(CancellationToken.None);

                    SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                    _fixture.CreateOrReplaceTable(conn, tableName, new[] { "src VARIANT" });

                    var cmd = conn.CreateCommand();
                    var rowCount = 0;

                    var insertCommand = $@"
-- borrowed from https://docs.snowflake.com/en/user-guide/querying-semistructured.html#sample-data-used-in-examples
insert into {tableName} (
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
                    IDataReader insertReader = await cmd.ExecuteReaderAsync();
                    Assert.Equal(500, insertReader.RecordsAffected);

                    var selectCommand = $"select * from {tableName}";
                    cmd.CommandText = selectCommand;
                    cmd.CommandType = System.Data.CommandType.Text;

                    rowCount = 0;
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            Newtonsoft.Json.JsonConvert.DeserializeObject(reader[0].ToString());
                            rowCount++;
                        }
                    }
                    Assert.Equal(500, rowCount);

                    SessionParameterAlterer.RestoreResultFormat(conn);
                    await conn.CloseAsync(CancellationToken.None);
                }
            }
            finally
            {
                ChunkParserFactory.Instance = previous;
            }
        }

        [SFFact]
        public async Task TestChunkRetry()
        {
            const int RetryFailureCount = 6;
            const int TestRowCount = 10000;
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

            var previous = ChunkParserFactory.Instance;
            var testFactory = new TestChunkParserFactory(RetryFailureCount);

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                try
                {
                    ChunkParserFactory.Instance = testFactory;
                    SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                    _fixture.CreateOrReplaceTable(conn, tableName, new[] { "col STRING" });

                    var cmd = conn.CreateCommand();
                    var rowCount = 0;

                    var insertCommand = $"insert into {tableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {TestRowCount})))";
                    cmd.CommandText = insertCommand;
                    IDataReader insertReader = await cmd.ExecuteReaderAsync();
                    Assert.Equal(TestRowCount, insertReader.RecordsAffected);

                    var selectCommand = $"select * from {tableName}";
                    cmd.CommandText = selectCommand;

                    rowCount = 0;
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var obj = new object[reader.FieldCount];
                            reader.GetValues(obj);
                            var val = obj[0] ?? System.String.Empty;
                            // Count rows that don't contain literal "u007f" or "\u007fu" strings
                            if (!val.ToString().Contains("u007f") && !val.ToString().Contains("\u007fu"))
                            {
                                rowCount++;
                            }
                        }
                    }
                    Assert.Equal(TestRowCount, rowCount);

                    Assert.True(testFactory.ExceptionsThrown >= RetryFailureCount);
                }
                finally
                {
                    ChunkParserFactory.Instance = previous;
                    SessionParameterAlterer.RestoreResultFormat(conn);
                    await conn.CloseAsync(CancellationToken.None);
                }
            }
        }

        [SFFact(Skip = "TODO INVESTIGATE")]
        public async Task TestExceptionThrownWhenChunkDownloadRetryCountExceeded()
        {
            const int ExcessiveRetryCount = 8;
            const int TestRowCount = 25000;
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

            var previous = ChunkParserFactory.Instance;

            try
            {
                ChunkParserFactory.Instance = new TestChunkParserFactory(ExcessiveRetryCount);

                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
                    await conn.OpenAsync(CancellationToken.None);

                    try
                    {
                        SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
                        _fixture.CreateOrReplaceTable(conn, tableName, new[] { "col STRING" });

                        var cmd = conn.CreateCommand();
                        var rowCount = 0;

                        var insertCommand = $"insert into {tableName}(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => {TestRowCount})))";
                        cmd.CommandText = insertCommand;
                        IDataReader insertReader = await cmd.ExecuteReaderAsync();
                        Assert.Equal(TestRowCount, insertReader.RecordsAffected);

                        var selectCommand = $"select * from {tableName}";
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
                        Assert.NotEqual(TestRowCount, rowCount);
                    }
                    finally
                    {
                        SessionParameterAlterer.RestoreResultFormat(conn);
                        await conn.CloseAsync(CancellationToken.None);
                    }
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
