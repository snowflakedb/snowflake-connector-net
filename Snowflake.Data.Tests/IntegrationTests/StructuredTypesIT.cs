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
        public void TestSelectInsertedStructuredTypeObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                CreateOrReplaceTable(connection, StructuredTypesTableName, new List<string> { "address OBJECT(city VARCHAR, state VARCHAR)" });
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"INSERT INTO {StructuredTypesTableName} SELECT {addressAsSFString}";
                    command.ExecuteNonQuery();
                    command.CommandText = $"SELECT * FROM {StructuredTypesTableName}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
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
        public void TestSelectArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('a','b','c')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { "a", "b", "c" }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfObjects()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfObjects =
                        "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('name', 'Alex'), OBJECT_CONSTRUCT('name', 'Brian'))::ARRAY(OBJECT(name VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfObjects}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<Identity>(0);

                    // assert
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<string[]>(0);

                    // assert
                    Assert.AreEqual(2, array.Length);
                    CollectionAssert.AreEqual(new[] { new[] { "a", "b" }, new[] { "c", "d" } }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfMap = "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(MAP(VARCHAR,VARCHAR))";
                    command.CommandText = $"SELECT {arrayOfMap}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<Dictionary<string, string>>(0);

                    // assert
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
        public void TestSelectMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA', 'zip', '01-234')::MAP(VARCHAR, VARCHAR)";
                    command.CommandText = $"SELECT {addressAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, string>(0);

                    // assert
                    Assert.NotNull(map);
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
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<int, string>(0);

                    // assert
                    Assert.NotNull(map);
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
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapSfString = "OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)";
                    command.CommandText = $"SELECT {mapSfString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<long, string>(0);

                    // assert
                    Assert.NotNull(map);
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
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWitObjectValueSFString = @"OBJECT_CONSTRUCT(
                        'Warsaw', OBJECT_CONSTRUCT('prefix', '01', 'postfix', '234'),
                        'San Mateo', OBJECT_CONSTRUCT('prefix', '02', 'postfix', '567')
                    )::MAP(VARCHAR, OBJECT(prefix VARCHAR, postfix VARCHAR))";
                    command.CommandText = $"SELECT {mapWitObjectValueSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, Zip>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.AreEqual(2, map.Count);
                    Assert.AreEqual(new Zip("01", "234"), map["Warsaw"]);
                    Assert.AreEqual(new Zip("02", "567"), map["San Mateo"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfArrays()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, string[]>(0);

                    // assert
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] {"a"}, map.Keys);
                    CollectionAssert.AreEqual(new string[] {"b", "c"}, map["a"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfLists()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, List<string>>(0);

                    // assert
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] {"a"}, map.Keys);
                    CollectionAssert.AreEqual(new string[] {"b", "c"}, map["a"]);
                }
            }
        }

        [Test]
        public void TestSelectMapOfMaps()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapAsSFString = "OBJECT_CONSTRUCT('a', OBJECT_CONSTRUCT('b', 'c'))::MAP(TEXT, MAP(TEXT, TEXT))";
                    command.CommandText = $"SELECT {mapAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, Dictionary<string, string>>(0);

                    // assert
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
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.AreEqual("abc", allUnstructuredTypesObject.StringValue);
                    Assert.AreEqual('x', allUnstructuredTypesObject.CharValue);
                    Assert.AreEqual(15, allUnstructuredTypesObject.ByteValue);
                    Assert.AreEqual(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.AreEqual(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.AreEqual(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.AreEqual(150150, allUnstructuredTypesObject.IntValue);
                    Assert.AreEqual(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.AreEqual(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allUnstructuredTypesObject.DecimalValue);
                    Assert.AreEqual(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.AreEqual(ConvertNewlinesOnWindows("{\n  \"a\": \"b\"\n}"), allUnstructuredTypesObject.SemiStructuredValue);
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
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.AreEqual("abc", allUnstructuredTypesObject.StringValue);
                    Assert.AreEqual('x', allUnstructuredTypesObject.CharValue);
                    Assert.AreEqual(15, allUnstructuredTypesObject.ByteValue);
                    Assert.AreEqual(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.AreEqual(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.AreEqual(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.AreEqual(150150, allUnstructuredTypesObject.IntValue);
                    Assert.AreEqual(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.AreEqual(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allUnstructuredTypesObject.DecimalValue);
                    Assert.AreEqual(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.AreEqual(ConvertNewlinesOnWindows("{\n  \"a\": \"b\"\n}"), allUnstructuredTypesObject.SemiStructuredValue);
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
                    var allUnstructuredTypesObject = reader.GetObject<AllNullableUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.IsNull(allUnstructuredTypesObject.StringValue);
                    Assert.IsNull(allUnstructuredTypesObject.CharValue);
                    Assert.IsNull(allUnstructuredTypesObject.ByteValue);
                    Assert.IsNull(allUnstructuredTypesObject.SByteValue);
                    Assert.IsNull(allUnstructuredTypesObject.ShortValue);
                    Assert.IsNull(allUnstructuredTypesObject.UShortValue);
                    Assert.IsNull(allUnstructuredTypesObject.IntValue);
                    Assert.IsNull(allUnstructuredTypesObject.UIntValue);
                    Assert.IsNull(allUnstructuredTypesObject.LongValue);
                    Assert.IsNull(allUnstructuredTypesObject.ULongValue);
                    Assert.IsNull(allUnstructuredTypesObject.FloatValue);
                    Assert.IsNull(allUnstructuredTypesObject.DoubleValue);
                    Assert.IsNull(allUnstructuredTypesObject.DecimalValue);
                    Assert.IsNull(allUnstructuredTypesObject.BooleanValue);
                    Assert.IsNull(allUnstructuredTypesObject.GuidValue);
                    Assert.IsNull(allUnstructuredTypesObject.DateTimeValue);
                    Assert.IsNull(allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.IsNull(allUnstructuredTypesObject.TimeSpanValue);
                    Assert.IsNull(allUnstructuredTypesObject.BinaryValue);
                    Assert.IsNull(allUnstructuredTypesObject.SemiStructuredValue);
                }
            }
        }

        [Test]
        public void TestSelectMapSkippingNullValues()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var mapAsSFString = "OBJECT_CONSTRUCT('a', NULL, 'b', '3')::MAP(VARCHAR, INTEGER)";
                    command.CommandText = $"SELECT {mapAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, int?>(0);

                    // assert
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
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfIntegers = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfIntegers}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<int>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3, 5, 8 }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfLong()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfLongs = "ARRAY_CONSTRUCT(3, 5, 8)::ARRAY(BIGINT)";
                    command.CommandText = $"SELECT {arrayOfLongs}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<long>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3L, 5L, 8L }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfFloats()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfFloats = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(FLOAT)";
                    command.CommandText = $"SELECT {arrayOfFloats}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<float>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3.1f, 5.2f, 8.11f }, array);
                }
            }
        }

        [Test]
        public void TestSelectArrayOfDoubles()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(3.1, 5.2, 8.11)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<double>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { 3.1d, 5.2d, 8.11d }, array);
                }
            }
        }

        [Test]
        [TestCaseSource(nameof(DateTimeConversionCases))]
        public void TestSelectDateTime(string dbValue, string dbType, DateTime? expectedRaw, DateTime expected)
        {
            // arrange
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
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act/assert
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
            // arrange
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
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act/assert
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
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('a',NULL,'b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new[] { "a", null, "b" }, array);
                }
            }
        }

        [Test]
        public void TestSelectIntArrayWithNulls()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfNumberSFString = "ARRAY_CONSTRUCT(3,NULL,5)::ARRAY(INTEGER)";
                    command.CommandText = $"SELECT {arrayOfNumberSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<int?>(0);

                    // assert
                    Assert.AreEqual(3, array.Length);
                    CollectionAssert.AreEqual(new int?[] { 3, null, 5 }, array);
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
        public void TestSelectNullArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullArraySFString = "NULL::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {nullArraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var nullArray = reader.GetArray<string>(0);

                    // assert
                    Assert.IsNull(nullArray);
                }
            }
        }

        [Test]
        public void TestSelectNullMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var nullMapSFString = "NULL::MAP(TEXT,TEXT)";
                    command.CommandText = $"SELECT {nullMapSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var nullMap = reader.GetMap<string, string>(0);

                    // assert
                    Assert.IsNull(nullMap);
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
                    Assert.AreEqual(SFError.STRUCTURED_TYPE_READ_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an object"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('x', 'y')::ARRAY";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<string>(0));

                    // assert
                    Assert.AreEqual(SFError.STRUCTURED_TYPE_READ_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an array"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var invalidMapSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {invalidMapSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, string>(0));

                    // assert
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
