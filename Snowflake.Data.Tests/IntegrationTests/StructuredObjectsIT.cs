using System.Collections.Generic;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class StructuredObjectIT : StructuredTypesIT
    {
        [Test]
        public void TestDataTableLoadOnStructuredObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
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
                        Assert.AreEqual($"{key}:{value}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [Test]
        public void TestSelectStructuredTypeObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var address = reader.GetObject<Address>(0);

                    // assert
                    Assert.AreEqual("San Mateo", address.city);
                    Assert.AreEqual("CA", address.state);
                    Assert.IsNull(address.zip);
                }
            }
        }

        [Test]
        public void TestSelectNestedStructuredTypeObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString =
                        "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', OBJECT_CONSTRUCT('prefix', '00', 'postfix', '11'))::OBJECT(city VARCHAR, state VARCHAR, zip OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var address = reader.GetObject<Address>(0);

                    // assert
                    Assert.AreEqual("San Mateo", address.city);
                    Assert.AreEqual("CA", address.state);
                    Assert.NotNull(address.zip);
                    Assert.AreEqual("00", address.zip.prefix);
                    Assert.AreEqual("11", address.zip.postfix);
                }
            }
        }

        [Test]
        public void TestSelectObjectWithMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithMap = "OBJECT_CONSTRUCT('names', OBJECT_CONSTRUCT('Excellent', '6', 'Poor', '1'))::OBJECT(names MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {objectWithMap}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var grades = reader.GetObject<GradesWithMap>(0);

                    // assert
                    Assert.NotNull(grades);
                    Assert.AreEqual(2, grades.Names.Count);
                    Assert.AreEqual("6", grades.Names["Excellent"]);
                    Assert.AreEqual("1", grades.Names["Poor"]);
                }
            }
        }

        [Test]
        public void TestSelectObjectWithArrays()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var grades = reader.GetObject<Grades>(0);

                    // assert
                    Assert.NotNull(grades);
                    CollectionAssert.AreEqual(new[] { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Test]
        public void TestSelectObjectWithList()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var grades = reader.GetObject<GradesWithList>(0);

                    // assert
                    Assert.NotNull(grades);
                    CollectionAssert.AreEqual(new List<string> { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Test]
        [TestCase(@"OBJECT_CONSTRUCT('Value', OBJECT_CONSTRUCT('a', 'b'))::OBJECT(Value OBJECT)", "{\"a\": \"b\"}")]
        [TestCase(@"OBJECT_CONSTRUCT('Value', ARRAY_CONSTRUCT('a', 'b'))::OBJECT(Value ARRAY)", "[\"a\", \"b\"]")]
        [TestCase(@"OBJECT_CONSTRUCT('Value', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::OBJECT(Value VARIANT)", "{\"a\": \"b\"}")]
        public void TestSelectSemiStructuredTypesInObject(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var wrapperObject = reader.GetObject<StringWrapper>(0);

                    // assert
                    Assert.NotNull(wrapperObject);
                    Assert.AreEqual(RemoveWhiteSpaces(expectedValue), RemoveWhiteSpaces(wrapperObject.Value));
                }
            }
        }

        [Test]
        public void TestSelectStructuredTypesAsNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
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
                    Assert.IsTrue(reader.Read());

                    // act
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);

                    // assert
                    Assert.NotNull(objectWithStructuredTypes);
                    Assert.IsNull(objectWithStructuredTypes.ObjectValue);
                    Assert.IsNull(objectWithStructuredTypes.ListValue);
                    Assert.IsNull(objectWithStructuredTypes.ArrayValue);
                    Assert.IsNull(objectWithStructuredTypes.IListValue);
                    Assert.IsNull(objectWithStructuredTypes.MapValue);
                    Assert.IsNull(objectWithStructuredTypes.IMapValue);
                }
            }
        }

        [Test]
        public void TestSelectNestedStructuredTypesNotNull()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
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
                    Assert.IsTrue(reader.Read());

                    // act
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);

                    // assert
                    Assert.NotNull(objectWithStructuredTypes);
                    Assert.AreEqual(new Identity("John"), objectWithStructuredTypes.ObjectValue);
                    CollectionAssert.AreEqual(new[] { "a", "b" }, objectWithStructuredTypes.ListValue);
                    CollectionAssert.AreEqual(new[] { "c" }, objectWithStructuredTypes.ArrayValue);
                    CollectionAssert.AreEqual(new[] { "d", "e" }, objectWithStructuredTypes.IListValue);
                    Assert.AreEqual(typeof(List<string>), objectWithStructuredTypes.IListValue.GetType());
                    Assert.AreEqual(1, objectWithStructuredTypes.MapValue.Count);
                    Assert.AreEqual(5, objectWithStructuredTypes.MapValue[3]);
                    Assert.AreEqual(1, objectWithStructuredTypes.IMapValue.Count);
                    Assert.AreEqual(13, objectWithStructuredTypes.IMapValue[8]);
                    Assert.AreEqual(typeof(Dictionary<int, int>), objectWithStructuredTypes.IMapValue.GetType());
                }
            }
        }

        [Test]
        public void TestRenamePropertyForPropertiesNamesConstruction()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'IntegerValue', '8',
                        'x', 'abc'
                    )::OBJECT(
                        IntegerValue INTEGER,
                        x TEXT
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesNamesConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.AreEqual("abc", objectForAnnotatedClass.StringValue);
                    Assert.IsNull(objectForAnnotatedClass.IgnoredValue);
                    Assert.AreEqual(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Test]
        public void TestIgnorePropertyForPropertiesOrderConstruction()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'x', 'abc',
                        'IntegerValue', '8'
                    )::OBJECT(
                        x TEXT,
                        IntegerValue INTEGER
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesOrderConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.AreEqual("abc", objectForAnnotatedClass.StringValue);
                    Assert.IsNull(objectForAnnotatedClass.IgnoredValue);
                    Assert.AreEqual(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Test]
        public void TestConstructorConstructionMethod()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = @"OBJECT_CONSTRUCT(
                        'x', 'abc',
                        'IntegerValue', '8'
                    )::OBJECT(
                        x TEXT,
                        IntegerValue INTEGER
                    )";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForConstructorConstruction>(0);

                    // assert
                    Assert.NotNull(objectForAnnotatedClass);
                    Assert.AreEqual("abc", objectForAnnotatedClass.StringValue);
                    Assert.IsNull(objectForAnnotatedClass.IgnoredValue);
                    Assert.AreEqual(8, objectForAnnotatedClass.IntegerValue);
                }
            }
        }

        [Test]
        public void TestSelectNullObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullObjectSFString = "NULL::OBJECT(Name TEXT)";
                    command.CommandText = $"SELECT {nullObjectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var nullObject = reader.GetObject<Identity>(0);

                    // assert
                    Assert.IsNull(nullObject);
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetObject<Identity>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an object"));
                    Assert.That(thrown.Message, Does.Contain("Method GetObject<Snowflake.Data.Tests.Client.Identity> can be used only for structured object"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidPropertyType()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = "OBJECT_CONSTRUCT('x', 'a', 'y', 'b')::OBJECT(x VARCHAR, y VARCHAR)";
                    command.CommandText = $"SELECT {objectSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetObject<AnnotatedClassForConstructorConstruction>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when reading path $[1]."));
                    Assert.That(thrown.Message, Does.Contain("Could not read text type into System.Int32"));
                }
            }
        }
    }
}
