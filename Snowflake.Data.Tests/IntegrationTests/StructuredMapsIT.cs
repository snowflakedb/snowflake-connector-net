using System;
using System.Collections.Generic;
using System.Data;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ResultFormat.ARROW, false)]
    [TestFixture(ResultFormat.ARROW, true)]
    [TestFixture(ResultFormat.JSON, false)]
    public class StructuredMapsIT : StructuredTypesIT
    {
        private readonly ResultFormat _resultFormat;
        private readonly bool _nativeArrow;

        public StructuredMapsIT(ResultFormat resultFormat, bool nativeArrow)
        {
            _resultFormat = resultFormat;
            _nativeArrow = nativeArrow;
        }

        [Test]
        public void TestDataTableLoadOnStructuredMap()
        {
            if (_resultFormat != ResultFormat.JSON)
                Assert.Ignore("skip test on arrow");

            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var key = "city";
                    var value = "San Mateo";
                    var addressAsSFString = $"OBJECT_CONSTRUCT('{key}','{value}')::MAP(VARCHAR, VARCHAR)";
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
        public void TestSelectMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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

                    if (_nativeArrow)
                    {
                        var arrowString = reader.GetString(0);
                        EnableStructuredTypes(connection, ResultFormat.JSON);
                        reader = (SnowflakeDbDataReader)command.ExecuteReader();
                        Assert.IsTrue(reader.Read());
                        var jsonString = reader.GetString(0);

                        Assert.IsTrue(JToken.DeepEquals(JObject.Parse(jsonString), JObject.Parse(arrowString)));
                    }
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, string[]>(0);

                    // assert
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] { "a" }, map.Keys);
                    CollectionAssert.AreEqual(new string[] { "b", "c" }, map["a"]);
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var mapWithArrayValueSFString = "OBJECT_CONSTRUCT('a', ARRAY_CONSTRUCT('b', 'c'))::MAP(VARCHAR, ARRAY(TEXT))";
                    command.CommandText = $"SELECT {mapWithArrayValueSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, List<string>>(0);

                    // assert
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(new string[] { "a" }, map.Keys);
                    CollectionAssert.AreEqual(new string[] { "b", "c" }, map["a"]);
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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
        [TestCase(@"OBJECT_CONSTRUCT('x', OBJECT_CONSTRUCT('a', 'b'))::MAP(VARCHAR,OBJECT)", "{\"a\": \"b\"}")]
        [TestCase(@"OBJECT_CONSTRUCT('x', ARRAY_CONSTRUCT('a', 'b'))::MAP(VARCHAR,ARRAY)", "[\"a\", \"b\"]")]
        [TestCase(@"OBJECT_CONSTRUCT('x', TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::MAP(VARCHAR,VARIANT)", "{\"a\": \"b\"}")]
        public void TestSelectSemiStructuredTypesInMap(string valueSfString, string expectedValue)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    command.CommandText = $"SELECT {valueSfString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var map = reader.GetMap<string, string>(0);

                    // assert
                    Assert.NotNull(map);
                    Assert.AreEqual(1, map.Count);
                    CollectionAssert.AreEqual(RemoveWhiteSpaces(expectedValue), RemoveWhiteSpaces(map["x"]));
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
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
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
        public void TestThrowExceptionForInvalidMap()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var invalidMapSFString = "OBJECT_CONSTRUCT('x', 'y')::OBJECT";
                    command.CommandText = $"SELECT {invalidMapSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, string>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting a map"));
                    Assert.That(thrown.Message, Does.Contain("Method GetMap<System.String, System.String> can be used only for structured map"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidMapElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection, _resultFormat, _nativeArrow);
                    var invalidMapSFString = @"OBJECT_CONSTRUCT(
                        'x', 'a76dacad-0e35-497b-bf9b-7cd49262b68b',
                        'y', 'z76dacad-0e35-497b-bf9b-7cd49262b68b'
                    )::MAP(TEXT,TEXT)";
                    command.CommandText = $"SELECT {invalidMapSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetMap<string, Guid>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                    if (_resultFormat == ResultFormat.JSON || !_nativeArrow)
                        Assert.That(thrown.Message, Does.Contain("Failed to read structured type when reading path $[1]"));
                    else
                        Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting a map."));
                }
            }
        }
    }
}
