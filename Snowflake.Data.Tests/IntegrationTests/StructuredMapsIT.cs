using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class StructuredMapsITJsonManaged : StructuredMapsIT
    {
        public StructuredMapsITJsonManaged(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.JSON, false) { }
    }

    public sealed class StructuredMapsITArrowManaged : StructuredMapsIT
    {
        public StructuredMapsITArrowManaged(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW, false) { }
    }

    public sealed class StructuredMapsITArrowNative : StructuredMapsIT
    {
        public StructuredMapsITArrowNative(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW, true) { }
    }

    public abstract class StructuredMapsIT : StructuredTypesIT
    {
        private readonly ResultFormat _resultFormat;
        private readonly bool _nativeArrow;

        private readonly SFBaseTestAsyncFixture _fixture;
        public StructuredMapsIT(SFBaseTestAsyncFixture fixture, ResultFormat resultFormat, bool nativeArrow) : base(fixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
            _nativeArrow = nativeArrow;
        }

        [SFFact]
        public async Task TestDataTableLoadOnStructuredMap()
        {
            if (_resultFormat != ResultFormat.JSON)
                Skip.When(true, "skip test on arrow");

            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var key = "city";
                    var value = "San Mateo";
                    var addressAsSFString = $"OBJECT_CONSTRUCT('{key}','{value}')::MAP(VARCHAR, VARCHAR)";
                    var colName = "colA";
                    command.CommandText = $"SELECT {addressAsSFString} AS {colName}";

                    // act
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dt = new DataTable();
                        dt.Load(reader);

                        // assert
                        Assert.Equal($"{key}:{value}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [SFFact]
        public async Task TestSelectMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', '01-234')::MAP(VARCHAR, VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.Equal(3, map.Count);
                    Assert.Equal("San Mateo", map["city"]);
                    Assert.Equal("CA", map["state"]);
                    Assert.Equal("01-234", map["zip"]);

                    if (_nativeArrow)
                    {
                        var arrowString = reader.GetString(0);
                        await EnableStructuredTypesAsync(connection, ResultFormat.JSON);
                        reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                        Assert.True(reader.Read());
                        var jsonString = reader.GetString(0);

                        Assert.True(JToken.DeepEquals(JObject.Parse(jsonString), JObject.Parse(arrowString)));
                    }
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapWithIntegerKeys()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<int, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.Equal(3, map.Count);
                    Assert.Equal("San Mateo", map[5]);
                    Assert.Equal("CA", map[8]);
                    Assert.Equal("01-234", map[13]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapWithLongKeys()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<long, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.Equal(3, map.Count);
                    Assert.Equal("San Mateo", map[5L]);
                    Assert.Equal("CA", map[8L]);
                    Assert.Equal("01-234", map[13L]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapOfObjects()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapWitObjectValueSFString = @"OBJECT_CONSTRUCT(
                        'Warsaw', OBJECT_CONSTRUCT('prefix', '01', 'postfix', '234'),
                        'San Mateo', OBJECT_CONSTRUCT('prefix', '02', 'postfix', '567')
                    )::MAP(VARCHAR, OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {mapWitObjectValueSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, Zip>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.Equal(2, map.Count);
                    Assert.Equal(new Zip("01", "234"), map["Warsaw"]);
                    Assert.Equal(new Zip("02", "567"), map["San Mateo"]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapOfArrays()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, string[]>(0);

                    // assert
                    Assert.Single(map);
                    Assert.Equal(new string[] { "a" }, map.Keys);
                    Assert.Equal(new string[] { "b", "c" }, map["a"]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapOfLists()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, List<string>>(0);

                    // assert
                    Assert.Single(map);
                    Assert.Equal(new string[] { "a" }, map.Keys);
                    Assert.Equal(new string[] { "b", "c" }, map["a"]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectMapOfMaps()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var mapAsSFString = "OBJECT_CONSTRUCT('a', OBJECT_CONSTRUCT('b', 'c'))::MAP(TEXT, MAP(TEXT, TEXT))";
                    command.CommandText = $"SELECT {mapAsSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, Dictionary<string, string>>(0);

                    // assert
                    Assert.Single(map);
                    var nestedMap = map["a"];
                    Assert.Single(nestedMap);
                    Assert.Equal("c", nestedMap["b"]);
                }
            }
        }

        [SFTheory]
        [InlineData(@"OBJECT_CONSTRUCT('x', OBJECT_CONSTRUCT('a', 'b'))::MAP(VARCHAR,OBJECT)", "{\"a\": \"b\"}")]
        [InlineData(@"OBJECT_CONSTRUCT('x', ARRAY_CONSTRUCT('a', 'b'))::MAP(VARCHAR,ARRAY)", "[\"a\", \"b\"]")]
        [InlineData(@"OBJECT_CONSTRUCT('x', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::MAP(VARCHAR,VARIANT)", "{\"a\": \"b\"}")]
        public async Task TestSelectSemiStructuredTypesInMap(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var map = reader.GetMap<string, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.Single(map);
                    Assert.Equal(RemoveWhiteSpaces(expectedValue), RemoveWhiteSpaces(map["x"]));
                }
            }
        }

        [SFFact]
        public async Task TestSelectNullMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var nullMapSFString = "NULL::MAP(TEXT,TEXT)";
                    command.CommandText = $"SELECT {nullMapSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var nullMap = reader.GetMap<string, string>(0);

                    // assert
                    Assert.Null(nullMap);
                }
            }
        }

        [SFFact]
        public async Task TestThrowExceptionForInvalidMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var invalidMapSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {invalidMapSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, string>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.Contains("Failed to read structured type when getting a map", thrown.Message);
                    Assert.Contains("Method GetMap<System.String, System.String> can be used only for structured map", thrown.Message);
                }
            }
        }

        [SFFact]
        public async Task TestThrowExceptionForInvalidMapElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var invalidMapSFString = @"OBJECT_CONSTRUCT(
                        'x', 'a76dacad-0e35-497b-bf9b-7cd49262b68b',
                        'y', 'z76dacad-0e35-497b-bf9b-7cd49262b68b'
                    )::MAP(TEXT,TEXT)";
                    command.CommandText = $"SELECT {invalidMapSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, Guid>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                    if (_resultFormat == ResultFormat.JSON || !_nativeArrow)
                        Assert.Contains("Failed to read structured type when reading path $[1]", thrown.Message);
                    else
                        Assert.Contains("Failed to read structured type when getting a map.", thrown.Message);
                }
            }
        }
    }
}
