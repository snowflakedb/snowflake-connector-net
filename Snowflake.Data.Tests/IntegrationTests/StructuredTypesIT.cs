using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Converter;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [IgnoreOnEnvIs("snowflake_cloud_env", new [] { "AZURE", "GCP" })]
    public class StructuredTypesIT : SFBaseTest
    {
        private const string StructuredTypesTableName = "structured_types_tests";

        [Test]
        public void TestInsertStructuredTypeObject()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                CreateOrReplaceTable(connection, StructuredTypesTableName, new List<string> { "address OBJECT(city VARCHAR, state VARCHAR)" });
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"INSERT INTO {StructuredTypesTableName} SELECT {addressAsSFString}";
                    command.ExecuteNonQuery();
                    command.CommandText = $"SELECT * FROM {StructuredTypesTableName}";

                    // act
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                }
            }
        }

        [Test]
        public void TestSelectStructuredTypeObject()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var address = reader.GetObject<Address>(0);
                    Assert.AreEqual("San Mateo", address.city);
                    Assert.AreEqual("CA", address.state);
                    Assert.IsNull(address.zip);
                }
            }
        }

        [Test]
        public void TestSelectNestedStructuredTypeObject()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString =
                        "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', OBJECT_CONSTRUCT('prefix', '00', 'postfix', '11'))::OBJECT(city VARCHAR, state VARCHAR, zip OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {addressAsSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var address = reader.GetObject<Address>(0);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithMap = "OBJECT_CONSTRUCT('names', OBJECT_CONSTRUCT('Excellent', '6', 'Poor', '1'))::OBJECT(names MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {objectWithMap}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var grades = reader.GetObject<GradesWithMap>(0);
                    Assert.NotNull(grades);
                    Assert.AreEqual(2, grades.Names.Count);
                    Assert.AreEqual("6", grades.Names["Excellent"]);
                    Assert.AreEqual("1", grades.Names["Poor"]);
                }
            }
        }

        [Test]
        public void TestSelectArray()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('a','b','c')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<string>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { "a", "b", "c" }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfObjects()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfObjects =
                        "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('name', 'Alex'), OBJECT_CONSTRUCT('name', 'Brian'))::ARRAY(OBJECT(name VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfObjects}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<Identity>(0);
                    Assert.AreEqual(2, array.Length);
                    CollectionAssert.AreEqual(new[] { new Identity("Alex"), new Identity("Brian") }, array);
                }
            }
        }


        [Test]
        public void TestSelectArrayOfArrays()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfArrays = "ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'), ARRAY_CONSTRUCT('c', 'd'))::ARRAY(ARRAY(TEXT))";
                    command.CommandText = $"SELECT {arrayOfArrays}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<string[]>(0);
                    Assert.AreEqual(2, array.Length);
                    CollectionAssert.AreEqual(new[] { new[] { "a", "b" }, new[] { "c", "d" } }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfMap()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfMap = "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfMap}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<Dictionary<string, string>>(0);
                    Assert.AreEqual(1, array.Length);
                    var map = array[0];
                    Assert.NotNull(map);
                    Assert.AreEqual(1, map.Count);
                    Assert.AreEqual("b",map["a"]);
                }
            }
        }

        [Test]
        public void TestSelectObjectWithArrays()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var grades = reader.GetObject<Grades>(0);
                    Assert.NotNull(grades);
                    CollectionAssert.AreEqual(new[] { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Test]
        public void TestSelectObjectWithList()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectWithArray = "OBJECT_CONSTRUCT('names', ARRAY_CONSTRUCT('Excellent', 'Poor'))::OBJECT(names ARRAY(TEXT))";
                    command.CommandText = $"SELECT {objectWithArray}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var grades = reader.GetObject<GradesWithList>(0);
                    Assert.NotNull(grades);
                    CollectionAssert.AreEqual(new List<string> { "Excellent", "Poor" }, grades.Names);
                }
            }
        }

        [Test]
        public void TestSelectMap()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', '01-234')::MAP(VARCHAR, VARCHAR)";
                    // var addressAsSFString = "{'city': 'San Mateo', 'state': 'CA'}::MAP(VARCHAR, VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, string>(0);
                    Assert.AreEqual(3, map.Count);
                    Assert.AreEqual("San Mateo", map["city"]);
                    Assert.AreEqual("CA", map["state"]);
                    Assert.AreEqual("01-234", map["zip"]);
                }
            }
        }

        [Test]
        public void TestSelectMapWithIntegerKeys()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<int, string>(0);
                    Assert.AreEqual(3, map.Count);
                    Assert.AreEqual("San Mateo", map[5]);
                    Assert.AreEqual("CA", map[8]);
                    Assert.AreEqual("01-234", map[13]);
                }
            }
        }

        [Test]
        public void TestSelectMapWithLongKeys()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<long, string>(0);
                    Assert.AreEqual(3, map.Count);
                    Assert.AreEqual("San Mateo", map[5L]);
                    Assert.AreEqual("CA", map[8L]);
                    Assert.AreEqual("01-234", map[13L]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfObjects()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWitObjectValueSFString = "OBJECT_CONSTRUCT('Warsaw', OBJECT_CONSTRUCT('prefix', '01', 'postfix', '234'), 'San Mateo', OBJECT_CONSTRUCT('prefix', '02', 'postfix', '567'))::MAP(VARCHAR, OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {mapWitObjectValueSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, Zip>(0);
                    Assert.AreEqual(2, map.Count);
                    Assert.AreEqual(new Zip("01", "234"), map["Warsaw"]);
                    Assert.AreEqual(new Zip("02", "567"), map["San Mateo"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfArrays()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, string[]>(0);
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] {"a"}, map.Keys);
                    CollectionAssert.AreEqual(new string[] {"b", "c"}, map["a"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfLists()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, List<string>>(0);
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] {"a"}, map.Keys);
                    CollectionAssert.AreEqual(new string[] {"b", "c"}, map["a"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfMaps()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapAsSFString = "OBJECT_CONSTRUCT('a', OBJECT_CONSTRUCT('b', 'c'))::MAP(TEXT, MAP(TEXT, TEXT))";
                    command.CommandText = $"SELECT {mapAsSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, Dictionary<string, string>>(0);
                    Assert.AreEqual(1, map.Count);
                    var nestedMap = map["a"];
                    Assert.AreEqual(1, nestedMap.Count);
                    Assert.AreEqual("c", nestedMap["b"]);
                }
            }
        }

        [Test]
        public void TestSelectAllUnstructuredTypesObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var timeZone = GetTimeZone(connection);
                    var expectedOffset = timeZone.GetUtcOffset(DateTime.Parse("2024-07-11 14:20:05"));
                    var expectedOffsetString = ToOffsetString(expectedOffset);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT(
                        'StringValue', 'abc',
                        'CharValue', 'x',
                        'ByteValue', 15,
                        'SByteValue', -14,
                        'ShortValue', 1200,
                        'UShortValue', 65000,
                        'IntValue', 150150,
                        'UIntValue', 151151,
                        'LongValue', 9111222333444555666,
                        'ULongValue', 9111222333444555666,
                        'FloatValue', 1.23,
                        'DoubleValue', 1.23,
                        'DecimalValue', 1.23,
                        'BooleanValue', true,
                        'GuidValue', '57af59a1-f010-450a-8c37-8fdc78e6ee93',
                        'DateTimeValue', '2024-07-11 14:20:05'::TIMESTAMP_NTZ,
                        'DateTimeOffsetValue', '2024-07-11 14:20:05'::TIMESTAMP_LTZ,
                        'TimeSpanValue', '14:20:05'::TIME,
                        'BinaryValue', TO_BINARY('this is binary data', 'UTF-8'),
                        'SemiStructuredValue', OBJECT_CONSTRUCT('a', 'b')
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    ), '2024-07-11 14:20:05'::TIMESTAMP_LTZ";
                    var bytesForBinary = Encoding.UTF8.GetBytes("this is binary data");
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var allTypesObject = reader.GetObject<AllTypesClass>(0);

                    // assert
                    Assert.NotNull(allTypesObject);
                    Assert.AreEqual("abc", allTypesObject.StringValue);
                    Assert.AreEqual('x', allTypesObject.CharValue);
                    Assert.AreEqual(15, allTypesObject.ByteValue);
                    Assert.AreEqual(-14, allTypesObject.SByteValue);
                    Assert.AreEqual(1200, allTypesObject.ShortValue);
                    Assert.AreEqual(65000, allTypesObject.UShortValue);
                    Assert.AreEqual(150150, allTypesObject.IntValue);
                    Assert.AreEqual(151151, allTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.AreEqual(1.23f, allTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allTypesObject.DecimalValue);
                    Assert.AreEqual(true, allTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allTypesObject.BinaryValue);
                    Assert.AreEqual(ConvertNewlinesOnWindows("{\n  \"a\": \"b\"\n}"), allTypesObject.SemiStructuredValue);
                }
            }
        }

        [Test]
        public void TestSelectAllUnstructuredTypesObjectIntoNullableFields()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var timeZone = GetTimeZone(connection);
                    var expectedOffset = timeZone.GetUtcOffset(DateTime.Parse("2024-07-11 14:20:05"));
                    var expectedOffsetString = ToOffsetString(expectedOffset);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT(
                        'StringValue', 'abc',
                        'CharValue', 'x',
                        'ByteValue', 15,
                        'SByteValue', -14,
                        'ShortValue', 1200,
                        'UShortValue', 65000,
                        'IntValue', 150150,
                        'UIntValue', 151151,
                        'LongValue', 9111222333444555666,
                        'ULongValue', 9111222333444555666,
                        'FloatValue', 1.23,
                        'DoubleValue', 1.23,
                        'DecimalValue', 1.23,
                        'BooleanValue', true,
                        'GuidValue', '57af59a1-f010-450a-8c37-8fdc78e6ee93',
                        'DateTimeValue', '2024-07-11 14:20:05'::TIMESTAMP_NTZ,
                        'DateTimeOffsetValue', '2024-07-11 14:20:05'::TIMESTAMP_LTZ,
                        'TimeSpanValue', '14:20:05'::TIME,
                        'BinaryValue', TO_BINARY('this is binary data', 'UTF-8'),
                        'SemiStructuredValue', OBJECT_CONSTRUCT('a', 'b')
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    )";
                    var bytesForBinary = Encoding.UTF8.GetBytes("this is binary data");
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var allTypesObject = reader.GetObject<AllTypesClass>(0);

                    // assert
                    Assert.NotNull(allTypesObject);
                    Assert.AreEqual("abc", allTypesObject.StringValue);
                    Assert.AreEqual('x', allTypesObject.CharValue);
                    Assert.AreEqual(15, allTypesObject.ByteValue);
                    Assert.AreEqual(-14, allTypesObject.SByteValue);
                    Assert.AreEqual(1200, allTypesObject.ShortValue);
                    Assert.AreEqual(65000, allTypesObject.UShortValue);
                    Assert.AreEqual(150150, allTypesObject.IntValue);
                    Assert.AreEqual(151151, allTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.AreEqual(1.23f, allTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allTypesObject.DecimalValue);
                    Assert.AreEqual(true, allTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allTypesObject.BinaryValue);
                    Assert.AreEqual(ConvertNewlinesOnWindows("{\n  \"a\": \"b\"\n}"), allTypesObject.SemiStructuredValue);
                }
            }
        }

        [Test]
        public void TestSelectNullIntoUnstructuredTypesObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT_KEEP_NULL(
                        'StringValue', NULL,
                        'CharValue', NULL,
                        'ByteValue', NULL,
                        'SByteValue', NULL,
                        'ShortValue', NULL,
                        'UShortValue', NULL,
                        'IntValue', NULL,
                        'UIntValue', NULL,
                        'LongValue', NULL,
                        'ULongValue', NULL,
                        'FloatValue', NULL,
                        'DoubleValue', NULL,
                        'DecimalValue', NULL,
                        'BooleanValue', NULL,
                        'GuidValue', NULL,
                        'DateTimeValue', NULL,
                        'DateTimeOffsetValue', NULL,
                        'TimeSpanValue', NULL,
                        'BinaryValue', NULL,
                        'SemiStructuredValue', NULL
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    )";
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var allTypesObject = reader.GetObject<AllNullableTypesClass>(0);

                    // assert
                    Assert.NotNull(allTypesObject);
                    Assert.IsNull(allTypesObject.StringValue);
                    Assert.IsNull(allTypesObject.CharValue);
                    Assert.IsNull(allTypesObject.ByteValue);
                    Assert.IsNull(allTypesObject.SByteValue);
                    Assert.IsNull(allTypesObject.ShortValue);
                    Assert.IsNull(allTypesObject.UShortValue);
                    Assert.IsNull(allTypesObject.IntValue);
                    Assert.IsNull(allTypesObject.UIntValue);
                    Assert.IsNull(allTypesObject.LongValue);
                    Assert.IsNull(allTypesObject.ULongValue);
                    Assert.IsNull(allTypesObject.FloatValue);
                    Assert.IsNull(allTypesObject.DoubleValue);
                    Assert.IsNull(allTypesObject.DecimalValue);
                    Assert.IsNull(allTypesObject.BooleanValue);
                    Assert.IsNull(allTypesObject.GuidValue);
                    Assert.IsNull(allTypesObject.DateTimeValue);
                    Assert.IsNull(allTypesObject.DateTimeOffsetValue);
                    Assert.IsNull(allTypesObject.TimeSpanValue);
                    Assert.IsNull(allTypesObject.BinaryValue);
                    Assert.IsNull(allTypesObject.SemiStructuredValue);
                }
            }
        }

        [Test]
        public void TestSelectMapSkippingNullValues()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapAsSFString = "OBJECT_CONSTRUCT('a', NULL, 'b', '3')::MAP(VARCHAR, INTEGER)";
                    command.CommandText = $"SELECT {mapAsSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var map = reader.GetMap<string, int?>(0);
                    Assert.AreEqual(1, map.Count);
                    Assert.AreEqual(3, map["b"]);
                }
            }
        }

        [Test]
        [TestCase(@"OBJECT_CONSTRUCT('Value', OBJECT_CONSTRUCT('a', 'b'))::OBJECT(Value OBJECT)", "{\n  \"a\": \"b\"\n}")]
        [TestCase(@"OBJECT_CONSTRUCT('Value', ARRAY_CONSTRUCT('a', 'b'))::OBJECT(Value ARRAY)", "[\n  \"a\",\n  \"b\"\n]")]
        [TestCase(@"OBJECT_CONSTRUCT('Value', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::OBJECT(Value VARIANT)", "{\n  \"a\": \"b\"\n}")]
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
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var wrapperObject = reader.GetObject<StringWrapper>(0);

                    // assert
                    Assert.NotNull(wrapperObject);
                    Assert.AreEqual(ConvertNewlinesOnWindows(expectedValue), wrapperObject.Value);
                }
            }
        }

        [Test]
        [TestCase(@"ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(OBJECT)", "{\n  \"a\": \"b\"\n}")]
        [TestCase(@"ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'))::ARRAY(ARRAY)", "[\n  \"a\",\n  \"b\"\n]")]
        [TestCase(@"ARRAY_CONSTRUCT(TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::ARRAY(VARIANT)", "{\n  \"a\": \"b\"\n}")]
        public void TestSelectSemiStructuredTypesInArray(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.NotNull(array);
                    CollectionAssert.AreEqual(new [] {ConvertNewlinesOnWindows(expectedValue)}, array);
                }
            }
        }

        [Test]
        [TestCase(@"OBJECT_CONSTRUCT('x', OBJECT_CONSTRUCT('a', 'b'))::MAP(VARCHAR,OBJECT)", "{\n  \"a\": \"b\"\n}")]
        [TestCase(@"OBJECT_CONSTRUCT('x', ARRAY_CONSTRUCT('a', 'b'))::MAP(VARCHAR,ARRAY)", "[\n  \"a\",\n  \"b\"\n]")]
        [TestCase(@"OBJECT_CONSTRUCT('x', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::MAP(VARCHAR,VARIANT)", "{\n  \"a\": \"b\"\n}")]
        public void TestSelectSemiStructuredTypesInMap(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(ConvertNewlinesOnWindows(expectedValue), map["x"]);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfIntegers()
        {
            using (var connection =
                   new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfIntegers = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfIntegers}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<int>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3, 5, 8 }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfLong()
        {
            using (var connection =
                   new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfLongs = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(BIGINT)";
                    command.CommandText = $"SELECT {arrayOfLongs}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<long>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3L, 5L, 8L }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfFloats()
        {
            using (var connection =
                   new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfFloats = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(FLOAT)";
                    command.CommandText = $"SELECT {arrayOfFloats}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<float>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3.1f, 5.2f, 8.11f }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfDoubles()
        {
            using (var connection =
                   new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<double>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3.1d, 5.2d, 8.11d }, array);
                }
            }
        }

        [Test]
        [TestCaseSource(nameof(DateTimeConversionCases))]
        public void TestSelectDateTime(string dbValue, string dbType, DateTime? expectedRaw, DateTime expected)
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.AreEqual(expectedRaw, rawValue);
                        Assert.AreEqual(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeWrapper>(1);
                    Assert.AreEqual(expected, wrappedValue.Value);
                    Assert.AreEqual(expected.Kind, wrappedValue.Value.Kind);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeConversionCases()
        {
            yield return new object[] { "2024-07-11 14:20:05", SFTimestampType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime() };
            yield return new object[] { "2024-07-11 14:20:05 +5:00", SFTimestampType.TIMESTAMP_TZ.ToString(), null, DateTime.Parse("2024-07-11 09:20:05").ToUniversalTime() };
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFTimestampType.TIMESTAMP_LTZ.ToString(), null, DateTime.Parse("2024-07-11 21:20:05").ToUniversalTime() };
            yield return new object[] { "2024-07-11 14:20:05.123456789", SFTimestampType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.1234567").ToUniversalTime(), DateTime.Parse("2024-07-11 14:20:05.1234568").ToUniversalTime()};
            yield return new object[] { "2024-07-11 14:20:05.123456789 +5:00", SFTimestampType.TIMESTAMP_TZ.ToString(), null, DateTime.Parse("2024-07-11 09:20:05.1234568").ToUniversalTime() };
            yield return new object[] {"2024-07-11 14:20:05.123456789 -7:00", SFTimestampType.TIMESTAMP_LTZ.ToString(), null, DateTime.Parse("2024-07-11 21:20:05.1234568").ToUniversalTime() };
        }

        [Test]
        [TestCaseSource(nameof(DateTimeOffsetConversionCases))]
        public void TestSelectDateTimeOffset(string dbValue, string dbType, DateTime? expectedRaw, DateTimeOffset expected)
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";

                    // act
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.AreEqual(expectedRaw, rawValue);
                        Assert.AreEqual(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeOffsetWrapper>(1);
                    Assert.AreEqual(expected, wrappedValue.Value);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeOffsetConversionCases()
        {
            yield return new object[] {"2024-07-11 14:20:05", SFTimestampType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime(), DateTimeOffset.Parse("2024-07-11 14:20:05Z")};
            yield return new object[] {"2024-07-11 14:20:05 +5:00", SFTimestampType.TIMESTAMP_TZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFTimestampType.TIMESTAMP_LTZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05 -7:00")};
            yield return new object[] {"2024-07-11 14:20:05.123456789", SFTimestampType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.1234567").ToUniversalTime(), DateTimeOffset.Parse("2024-07-11 14:20:05.1234568Z")};
            yield return new object[] {"2024-07-11 14:20:05.123456789 +5:00", SFTimestampType.TIMESTAMP_TZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05.1234568 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05.123456789 -7:00", SFTimestampType.TIMESTAMP_LTZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05.1234568 -7:00")};
        }

        [Test]
        public void TestSelectStringArrayWithNulls()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('a',NULL,'b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<string>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { "a", null, "b" }, array);
                }
            }
        }

        [Test]
        public void TestSelectIntArrayWithNulls()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfNumberSFString = "ARRAY_CONSTRUCT(3,NULL,5)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfNumberSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var array = reader.GetArray<int?>(0);
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new int?[] { 3, null, 5 }, array);
                }
            }
        }

        [Test]
        public void TestSelectStructuredTypesAsNulls()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
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

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
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

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var objectWithStructuredTypes = reader.GetObject<ObjectArrayMapWrapper>(0);
                    Assert.NotNull(objectWithStructuredTypes);
                    Assert.AreEqual(new Identity("John"), objectWithStructuredTypes.ObjectValue);
                    CollectionAssert.AreEqual(new [] {"a", "b"}, objectWithStructuredTypes.ListValue);
                    CollectionAssert.AreEqual(new [] {"c"}, objectWithStructuredTypes.ArrayValue);
                    CollectionAssert.AreEqual(new [] {"d", "e"}, objectWithStructuredTypes.IListValue);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
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

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesNamesConstruction>(0);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
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

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForPropertiesOrderConstruction>(0);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
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

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    var objectForAnnotatedClass = reader.GetObject<AnnotatedClassForConstructorConstruction>(0);
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
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullObjectSFString = "NULL::OBJECT(Name TEXT)";
                    command.CommandText = $"SELECT {nullObjectSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var nullObject = reader.GetObject<Identity>(0);
                    Assert.IsNull(nullObject);
                }
            }
        }

        [Test]
        public void TestSelectNullArray()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullArraySFString = "NULL::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {nullArraySFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var nullArray = reader.GetArray<string>(0);
                    Assert.IsNull(nullArray);
                }
            }
        }

        [Test]
        public void TestSelectNullMap()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullMapSFString = "NULL::MAP(TEXT,TEXT)";
                    command.CommandText = $"SELECT {nullMapSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var nullMap = reader.GetMap<string, string>(0);
                    Assert.IsNull(nullMap);
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidObject()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var objectSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {objectSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetObject<Identity>(0));
                    Assert.AreEqual(SFError.STRUCTURED_TYPE_READ_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an object"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidArray()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('x', 'y')::ARRAY";
                    command.CommandText = $"SELECT {arraySFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<string>(0));
                    Assert.AreEqual(SFError.STRUCTURED_TYPE_READ_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an array"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidMap()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var invalidMapSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {invalidMapSFString}";

                    // act
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    // assert
                    Assert.IsTrue(reader.Read());
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, string>(0));
                    Assert.AreEqual(SFError.STRUCTURED_TYPE_READ_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting a map"));
                }
            }
        }

        private TimeZoneInfo GetTimeZone(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "show parameters like 'timezone'";
                var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                var timeZoneString = reader.GetString(1);
                return TimeZoneInfoConverter.FindSystemTimeZoneById(timeZoneString);
            }
        }

        private string ToOffsetString(TimeSpan timeSpan)
        {
            var offsetString = timeSpan.ToString();
            var secondsIndex = offsetString.LastIndexOf(":");
            var offsetWithoutSeconds = offsetString.Substring(0, secondsIndex);
            return offsetWithoutSeconds.StartsWith("+") || offsetWithoutSeconds.StartsWith("-")
                ? offsetWithoutSeconds
                : "+" + offsetWithoutSeconds;
        }

        private void SetTimePrecision(SnowflakeDbConnection connection, int precision)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF{precision}'";
                command.ExecuteNonQuery();
                command.CommandText = $"ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF{precision} TZHTZM'";
                command.ExecuteNonQuery();

            }
        }

        private void EnableStructuredTypes(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT=JSON";
                command.ExecuteNonQuery();
            }
        }

        private string ConvertNewlinesOnWindows(string text) =>
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? text.Replace("\n", "\r\n")
                : text;
    }
}
