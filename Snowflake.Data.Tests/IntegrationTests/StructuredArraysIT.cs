using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
        public StructuredArraysITJsonManaged(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture, ResultFormat.JSON, false) { }
    }

    public sealed class StructuredArraysITArrowManaged : StructuredArraysIT
    {
        public StructuredArraysITArrowManaged(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture, ResultFormat.ARROW, false) { }
    }

    public sealed class StructuredArraysITArrowNative : StructuredArraysIT
    {
        public StructuredArraysITArrowNative(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture, ResultFormat.ARROW, true) { }
    }

    public abstract class StructuredArraysIT : StructuredTypesIT
    {
        private readonly ResultFormat _resultFormat;
        private readonly bool _nativeArrow;

        private readonly SFBaseTestAsyncFixture _fixture;
        public StructuredArraysIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture, ResultFormat resultFormat, bool nativeArrow) : base(fixture, envFixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
            _nativeArrow = nativeArrow;
        }

        [Fact]
        public void TestDataTableLoadOnStructuredArrayJsonFormat()
        {
            if (_resultFormat != ResultFormat.JSON)
                Skip.If(true, "skip test on arrow");

            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var expectedValueA = 'a';
                    var expectedValueB = 'b';
                    var expectedValueC = 'c';
                    var arraySFString = $"ARRAY_CONSTRUCT('{expectedValueA}','{expectedValueB}','{expectedValueC}')::ARRAY(TEXT)";
                    var colName = "colA";
                    command.CommandText = $"SELECT {arraySFString} AS {colName}";

                    // act
                    using (var reader = command.ExecuteReader())
                    {
                        var dt = new DataTable();
                        dt.Load(reader);

                        // assert
                        Assert.Equal($"{expectedValueA},{expectedValueB},{expectedValueC}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [Fact]
        public void TestSelectArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a','b','c')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.Equal(3, array.Length);
                    Assert.Equal(new[] { "a", "b", "c" }, array);

                    if (_nativeArrow)
                    {
                        var arrowString = reader.GetString(0);
                        EnableStructuredTypes(connection, ResultFormat.JSON);
                        reader = (SnowflakeDbDataReader)command.ExecuteReader();
                        Assert.True(reader.Read());
                        var jsonString = reader.GetString(0);

                        Assert.True(JToken.DeepEquals(JArray.Parse(jsonString), JArray.Parse(arrowString)));
                    }
                }
            }
        }

        [Fact]
        public void TestSelectArrayOfObjects()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfObjects =
                        "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('name', 'Alex'), OBJECT_CONSTRUCT('name', 'Brian'))::ARRAY(OBJECT(name VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfObjects}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfArrays()
        {
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfArrays = "ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'), ARRAY_CONSTRUCT('c', 'd'))::ARRAY(ARRAY(TEXT))";
                    command.CommandText = $"SELECT {arrayOfArrays}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfMap = "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfMap}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectSemiStructuredTypesInArray(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfIntegers()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfIntegers = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfIntegers}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfLong()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfLongs = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(BIGINT)";
                    command.CommandText = $"SELECT {arrayOfLongs}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfFloats()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfFloats = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(FLOAT)";
                    command.CommandText = $"SELECT {arrayOfFloats}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfDoubles()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfDoublesWithExponentNotation()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(1.0e100, 1.0e-100)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfBooleans()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfBooleans = "ARRAY_CONSTRUCT(true, false)::ARRAY(BOOLEAN)";
                    command.CommandText = $"SELECT {arrayOfBooleans}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfBinaries()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfBinaries = "ARRAY_CONSTRUCT(TO_BINARY('AB', 'UTF-8'), TO_BINARY('BC', 'UTF-8'))::ARRAY(BINARY)";
                    command.CommandText = $"SELECT {arrayOfBinaries}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfDates()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfDates = "ARRAY_CONSTRUCT('2024-01-01'::DATE)::ARRAY(DATE)";
                    command.CommandText = $"SELECT {arrayOfDates}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectStringArrayWithNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a',NULL,'b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectIntArrayWithNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arrayOfNumberSFString = "ARRAY_CONSTRUCT(3,NULL,5)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfNumberSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectNullArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var nullArraySFString = "NULL::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {nullArraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var nullArray = reader.GetArray<string>(0);

                    // assert
                    Assert.Null(nullArray);
                }
            }
        }

        [Fact]
        public void TestThrowExceptionForInvalidArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('x', 'y')::ARRAY";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestThrowExceptionForInvalidArrayElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arraySFString = "ARRAY_CONSTRUCT('a76dacad-0e35-497b-bf9b-7cd49262b68b', 'z76dacad-0e35-497b-bf9b-7cd49262b68b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestThrowExceptionForNextedInvalidElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var arraySFString = @"ARRAY_CONSTRUCT(
                        OBJECT_CONSTRUCT('x', 'a', 'y', 'b')
                    )::ARRAY(OBJECT(x VARCHAR, y VARCHAR))";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
