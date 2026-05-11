using System.Collections.Generic;
using System.Data;
using Newtonsoft.Json.Linq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class StructuredObjectIT : StructuredTypesIT
    {
        private readonly ResultFormat _resultFormat;
        private readonly bool _nativeArrow;

        private readonly SFBaseTestAsyncFixture _fixture;
        public StructuredObjectIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture, ResultFormat resultFormat, bool nativeArrow) : base(fixture, envFixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
            _nativeArrow = nativeArrow;
        }

        [Fact]
        public void TestDataTableLoadOnStructuredObject()
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
                    var key = "city";
                    var value = "San Mateo";
                    var addressAsSFString = $"OBJECT_CONSTRUCT('{key}','{value}')::OBJECT(city VARCHAR)";
                    var colName = "colA";
                    command.CommandText = $"SELECT {addressAsSFString} AS {colName}";

                    // act
                    using (var reader = command.ExecuteReader())
                    {
                        var dt = new DataTable();
                        dt.Load(reader);

                        // assert
                        Assert.Equal($"{key}:{value}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [Fact]
        public void TestSelectStructuredTypeObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var address = reader.GetObject<Address>(0);

                    // assert
                    Assert.Equal("San Mateo", address.city);
                    Assert.Equal("CA", address.state);
                    Assert.Null(address.zip);

                    if (_nativeArrow)
                    {
                        var arrowString = reader.GetString(0);
                        EnableStructuredTypes(connection, ResultFormat.JSON);
                        reader = (SnowflakeDbDataReader)command.ExecuteReader();
                        Assert.True(reader.Read());
                        var jsonString = reader.GetString(0);

                        Assert.True(JToken.DeepEquals(JObject.Parse(jsonString), JObject.Parse(arrowString)));
                    }
                }
            }
        }

        [Fact]
        public void TestSelectNestedStructuredTypeObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var addressAsSFString =
                        "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', OBJECT_CONSTRUCT('prefix', '00', 'postfix', '11'))::OBJECT(city VARCHAR, state VARCHAR, zip OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var address = reader.GetObject<Address>(0);

                    // assert
                    Assert.Equal("San Mateo", address.city);
                    Assert.Equal("CA", address.state);
                    Assert.NotNull(address.zip);
                    Assert.Equal("00", address.zip.prefix);
                    Assert.Equal("11", address.zip.postfix);
                }
            }
        }

        [Fact]
        public void TestSelectObjectWithMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectWithMap = "OBJECT_CONSTRUCT('names', OBJECT_CONSTRUCT('Excellent', '6', 'Poor', '1'))::OBJECT(names MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {objectWithMap}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var grades = reader.GetObject<GradesWithMap>(0);

                    // assert
                    Assert.NotNull(grades);
                    Assert.Equal(2, grades.Names.Count);
                    Assert.Equal("6", grades.Names["Excellent"]);
                    Assert.Equal("1", grades.Names["Poor"]);
                }
            }
        }

        [Fact]
        public void TestSelectObjectWithArrays()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var grades = reader.GetObject<Grades>(0);

                    // assert
                    Assert.NotNull(grades);
                    Assert.Equal(new[] { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Fact]
        public void TestSelectObjectWithList()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var grades = reader.GetObject<GradesWithList>(0);

                    // assert
                    Assert.NotNull(grades);
                    Assert.Equal(new List<string> { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Theory]
        [InlineData(@"OBJECT_CONSTRUCT('Value', OBJECT_CONSTRUCT('a', 'b'))::OBJECT(Value OBJECT)", "{\"a\": \"b\"}")]
        [InlineData(@"OBJECT_CONSTRUCT('Value', ARRAY_CONSTRUCT('a', 'b'))::OBJECT(Value ARRAY)", "[\"a\", \"b\"]")]
        [InlineData(@"OBJECT_CONSTRUCT('Value', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::OBJECT(Value VARIANT)", "{\"a\": \"b\"}")]
        public void TestSelectSemiStructuredTypesInObject(string valueSfString, string expectedValue)
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
                    var wrapperObject = reader.GetObject<StringWrapper>(0);

                    // assert
                    Assert.NotNull(wrapperObject);
                    Assert.Equal(RemoveWhiteSpaces(expectedValue), RemoveWhiteSpaces(wrapperObject.Value));
                }
            }
        }

        [Fact]
        public void TestSelectStructuredTypesAsNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = @"OBJECT_CONSTRUCT_KEEP_NULL(
                        'ObjectValue', NULL,
                        'ListValue', NULL,
                        'ArrayValue', NULL,
                        'IListValue', NULL,
                        'MapValue', NULL,
                        'IMapValue', NULL
                    )::OBJECT(
                        ObjectValue OBJECT(Name TEXT),
                        ListValue ARRAY(TEXT),
                        ArrayValue ARRAY(TEXT),
                        IListValue ARRAY(TEXT),
                        MapValue MAP(INTEGER, INTEGER),
                        IMapValue MAP(INTEGER, INTEGER)
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);

                    // assert
                    Assert.NotNull(objectWithStructuredTypes);
                    Assert.Null(objectWithStructuredTypes.ObjectValue);
                    Assert.Null(objectWithStructuredTypes.ListValue);
                    Assert.Null(objectWithStructuredTypes.ArrayValue);
                    Assert.Null(objectWithStructuredTypes.IListValue);
                    Assert.Null(objectWithStructuredTypes.MapValue);
                    Assert.Null(objectWithStructuredTypes.IMapValue);
                }
            }
        }

        [Fact]
        public void TestSelectNestedStructuredTypesNotNull()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = @"OBJECT_CONSTRUCT_KEEP_NULL(
                        'ObjectValue', OBJECT_CONSTRUCT('Name', 'John'),
                        'ListValue', ARRAY_CONSTRUCT('a', 'b'),
                        'ArrayValue', ARRAY_CONSTRUCT('c'),
                        'IListValue', ARRAY_CONSTRUCT('d', 'e'),
                        'MapValue', OBJECT_CONSTRUCT('3', '5'),
                        'IMapValue', OBJECT_CONSTRUCT('8', '13')
                    )::OBJECT(
                        ObjectValue OBJECT(Name TEXT),
                        ListValue ARRAY(TEXT),
                        ArrayValue ARRAY(TEXT),
                        IListValue ARRAY(TEXT),
                        MapValue MAP(INTEGER, INTEGER),
                        IMapValue MAP(INTEGER, INTEGER)
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);

                    // assert
                    Assert.NotNull(objectWithStructuredTypes);
                    Assert.Equal(new Identity("John"), objectWithStructuredTypes.ObjectValue);
                    Assert.Equal(new[] { "a", "b" }, objectWithStructuredTypes.ListValue);
                    Assert.Equal(new[] { "c" }, objectWithStructuredTypes.ArrayValue);
                    Assert.Equal(new[] { "d", "e" }, objectWithStructuredTypes.IListValue);
                    Assert.Equal(typeof(List<string>), objectWithStructuredTypes.IListValue.GetType());
                    Assert.Equal(1, objectWithStructuredTypes.MapValue.Count);
                    Assert.Equal(5, objectWithStructuredTypes.MapValue[3]);
                    Assert.Equal(1, objectWithStructuredTypes.IMapValue.Count);
                    Assert.Equal(13, objectWithStructuredTypes.IMapValue[8]);
                    Assert.Equal(typeof(Dictionary<int, int>), objectWithStructuredTypes.IMapValue.GetType());
                }
            }
        }

        [Fact]
        public void TestRenamePropertyForPropertiesNamesConstruction()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'IntegerValue', '8',
                        'x', 'abc'
                    )::OBJECT(
                        IntegerValue INTEGER,
                        x TEXT
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesNamesConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.Equal("abc", objectForAnnotatedClass.StringValue);
                    Assert.Null(objectForAnnotatedClass.IgnoredValue);
                    Assert.Equal(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Fact]
        public void TestIgnorePropertyForPropertiesOrderConstruction()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'x', 'abc',
                        'IntegerValue', '8'
                    )::OBJECT(
                        x TEXT,
                        IntegerValue INTEGER
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesOrderConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.Equal("abc", objectForAnnotatedClass.StringValue);
                    Assert.Null(objectForAnnotatedClass.IgnoredValue);
                    Assert.Equal(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Fact]
        public void TestConstructorConstructionMethod()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'x', 'abc',
                        'IntegerValue', '8'
                    )::OBJECT(
                        x TEXT,
                        IntegerValue INTEGER
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForConstructorConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.Equal("abc", objectForAnnotatedClass.StringValue);
                    Assert.Null(objectForAnnotatedClass.IgnoredValue);
                    Assert.Equal(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Fact]
        public void TestSelectNullObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var nullObjectSFString = "NULL::OBJECT(Name TEXT)";
                    command.CommandText = $"SELECT {nullObjectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var nullObject = reader.GetObject<Identity>(0);

                    // assert
                    Assert.Null(nullObject);
                }
            }
        }

        [Fact]
        public void TestThrowExceptionForInvalidObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetObject<Identity>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.Contains("Failed to read structured type when getting an object", thrown.Message);
                    Assert.Contains("Method GetObject<Snowflake.Data.Tests.Client.Identity> can be used only for structured object", thrown.Message);
                }
            }
        }

        [Fact]
        public void TestThrowExceptionForInvalidPropertyType()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var objectSFString = "OBJECT_CONSTRUCT('x', 'a', 'y', 'b')::OBJECT(x VARCHAR, y VARCHAR)";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetObject<AnnotatedClassForConstructorConstruction>(0));

                    // assert
                    if (_resultFormat == ResultFormat.JSON || !_nativeArrow)
                    {
                        SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                        Assert.Contains("Failed to read structured type when reading path $[1].", thrown.Message);
                        Assert.Contains("Could not read text type into System.Int32", thrown.Message);
                    }
                    else
                    {
                        SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                        Assert.Contains("Failed to read structured type when getting an object.", thrown.Message);
                    }
                }
            }
        }
    }
}
