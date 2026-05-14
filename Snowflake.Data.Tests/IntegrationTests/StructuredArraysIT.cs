using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
    public sealed class StructuredArraysITJsonManaged : StructuredArraysIT
    {
        public StructuredArraysITJsonManaged(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.JSON, false) { }

        [Fact]
        public async Task TestDataTableLoadOnStructuredArrayJsonFormat()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var expectedValueA = 'a';
                    var expectedValueB = 'b';
                    var expectedValueC = 'c';
                    var arraySFString = $"ARRAY_CONSTRUCT('{expectedValueA}','{expectedValueB}','{expectedValueC}')::ARRAY(TEXT)";
                    var colName = "colA";
                    command.CommandText = $"SELECT {arraySFString} AS {colName}";

                    // act
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dt = new DataTable();
                        dt.Load(reader);

                        // assert
                        Assert.Equal($"{expectedValueA},{expectedValueB},{expectedValueC}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

    }

    public sealed class StructuredArraysITArrowManaged : StructuredArraysIT
    {
        public StructuredArraysITArrowManaged(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW, false) { }
    }

    public sealed class StructuredArraysITArrowNative : StructuredArraysIT
    {
        public StructuredArraysITArrowNative(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW, true) { }
    }

    public abstract class StructuredArraysIT : StructuredTypesIT
    {
        protected readonly ResultFormat _resultFormat;
        protected readonly bool _nativeArrow;

        protected readonly SFBaseTestAsyncFixture _fixture;
        public StructuredArraysIT(SFBaseTestAsyncFixture fixture, ResultFormat resultFormat, bool nativeArrow) : base(fixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
            _nativeArrow = nativeArrow;
        }


        [Fact]
        public async Task TestSelectArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a','b','c')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { "a", "b", "c" }, array);

                    if (_nativeArrow)
                    {
                        var arrowString = reader.GetString(0);
                        await EnableStructuredTypesAsync(connection, ResultFormat.JSON);
                        reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                        Assert.True(reader.Read());
                        var jsonString = reader.GetString(0);

                        Assert.True(JToken.DeepEquals(JArray.Parse(jsonString), JArray.Parse(arrowString)));
                    }
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfObjects()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfObjects =
                        "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('name', 'Alex'), OBJECT_CONSTRUCT('name', 'Brian'))::ARRAY(OBJECT(name VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfObjects}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<Identity>(0);

                    // assert
                    Assert.Equal(2, array.Length);
                    Assert.Equal(new[] { new Identity("Alex"), new Identity("Brian") }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfArrays()
        {
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                // arrange
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfArrays = "ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'), ARRAY_CONSTRUCT('c', 'd'))::ARRAY(ARRAY(TEXT))";
                    command.CommandText = $"SELECT {arrayOfArrays}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<string[]>(0);

                    // assert
                    Assert.Equal(2, array.Length);
                    Assert.Equal(new[] { new[] { "a", "b" }, new[] { "c", "d" } }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfMap = "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfMap}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<Dictionary<string, string>>(0);

                    // assert
                    Assert.Equal(1, array.Length);
                    var map = array[0];
                    Assert.NotNull(map);
                    Assert.Equal(1, map.Count);
                    Assert.Equal("b", map["a"]);
                }
            }
        }

        [Theory]
        [InlineData(@"ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(OBJECT)", "{\"a\": \"b\"}")]
        [InlineData(@"ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'))::ARRAY(ARRAY)", "[\"a\", \"b\"]")]
        [InlineData(@"ARRAY_CONSTRUCT(TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::ARRAY(VARIANT)", "{\"a\": \"b\"}")]
        public async Task TestSelectSemiStructuredTypesInArray(string valueSfString, string expectedValue)
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
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.NotNull(array);
                    Assert.Equal(new[] { RemoveWhiteSpaces(expectedValue) }, array.Select(RemoveWhiteSpaces).ToArray());
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfIntegers()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfIntegers = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfIntegers}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<int>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { 3, 5, 8 }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfLong()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfLongs = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(BIGINT)";
                    command.CommandText = $"SELECT {arrayOfLongs}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<long>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { 3L, 5L, 8L }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfFloats()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfFloats = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(FLOAT)";
                    command.CommandText = $"SELECT {arrayOfFloats}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<float>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { 3.1f, 5.2f, 8.11f }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfDoubles()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<double>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { 3.1d, 5.2d, 8.11d }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfDoublesWithExponentNotation()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(1.0e100, 1.0e-100)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<double>(0);

                    // assert
                    Assert.Equal(2, array.Length);
                    Assert.Equal(new[] { 1.0e100d, 1.0e-100d }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfBooleans()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfBooleans = "ARRAY_CONSTRUCT(true, false)::ARRAY(BOOLEAN)";
                    command.CommandText = $"SELECT {arrayOfBooleans}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<bool>(0);

                    // assert
                    Assert.Equal(2, array.Length);
                    Assert.Equal(new[] { true, false }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfBinaries()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfBinaries = "ARRAY_CONSTRUCT(TO_BINARY('AB', 'UTF-8'), TO_BINARY('BC', 'UTF-8'))::ARRAY(BINARY)";
                    command.CommandText = $"SELECT {arrayOfBinaries}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<byte[]>(0);
                    var strings = array.Select(b => Encoding.UTF8.GetString(b)).ToArray();

                    // assert
                    Assert.Equal(2, array.Length);
                    Assert.Equal(new[] { "AB", "BC" }, strings);
                }
            }
        }

        [Fact]
        public async Task TestSelectArrayOfDates()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfDates = "ARRAY_CONSTRUCT('2024-01-01'::DATE)::ARRAY(DATE)";
                    command.CommandText = $"SELECT {arrayOfDates}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<DateTime>(0);

                    // assert
                    Assert.Equal(1, array.Length);
                    Assert.Equal(new[] { DateTime.Parse("2024-01-01") }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectStringArrayWithNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a',NULL,'b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { "a", null, "b" }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectIntArrayWithNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arrayOfNumberSFString = "ARRAY_CONSTRUCT(3,NULL,5)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfNumberSFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<int?>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new int?[] { 3, null, 5 }, array);
                }
            }
        }

        [Fact]
        public async Task TestSelectNullArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var nullArraySFString = "NULL::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {nullArraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var nullArray = reader.GetArray<string>(0);

                    // assert
                    Assert.Null(nullArray);
                }
            }
        }

        [Fact]
        public async Task TestThrowExceptionForInvalidArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('x', 'y')::ARRAY";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<string>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.Contains("Failed to read structured type when getting an array", thrown.Message);
                    Assert.Contains("Method GetArray<System.String> can be used only for structured array", thrown.Message);
                }
            }
        }

        [Fact]
        public async Task TestThrowExceptionForInvalidArrayElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a76dacad-0e35-497b-bf9b-7cd49262b68b', 'z76dacad-0e35-497b-bf9b-7cd49262b68b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<Guid>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                    if (_resultFormat == ResultFormat.JSON || !_nativeArrow)
                        Assert.Contains("Failed to read structured type when reading path $[1]", thrown.Message);
                    else
                        Assert.Contains("Failed to read structured type when getting an array.", thrown.Message);

                }
            }
        }

        [Fact]
        public async Task TestThrowExceptionForNextedInvalidElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None);
                using (var command = connection.CreateCommand())
                {
                    await EnableStructuredTypesAsync(connection, _resultFormat, _nativeArrow);
                    var arraySFString = @"ARRAY_CONSTRUCT(
                        OBJECT_CONSTRUCT('x', 'a', 'y', 'b')
                    )::ARRAY(OBJECT(x VARCHAR, y VARCHAR))";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<AnnotatedClassForConstructorConstruction>(0));

                    // assert
                    if (_resultFormat == ResultFormat.JSON || !_nativeArrow)
                    {
                        SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                        Assert.Contains("Failed to read structured type when reading path $[0][1]", thrown.Message);
                        Assert.Contains("Could not read text type into System.Int32", thrown.Message);
                    }
                    else
                    {
                        SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                        Assert.Contains("Failed to read structured type when getting an array.", thrown.Message);
                    }
                }
            }
        }

    }
}
