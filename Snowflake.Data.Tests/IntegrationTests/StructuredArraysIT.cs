using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class StructuredArraysIT : StructuredTypesIT
    {
        [Test]
        public void TestDataTableLoadOnStructuredArray()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
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
                        Assert.AreEqual($"{expectedValueA},{expectedValueB},{expectedValueC}", DataTableParser.GetFirstRowValue(dt, colName));
                    }
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
                    Assert.AreEqual("b", map["a"]);
                }
            }
        }

        [Test]
        [TestCase(@"ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('a', 'b'))::ARRAY(OBJECT)", "{\"a\": \"b\"}")]
        [TestCase(@"ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'))::ARRAY(ARRAY)", "[\"a\", \"b\"]")]
        [TestCase(@"ARRAY_CONSTRUCT(TO_VARIANT(OBJECT_CONSTRUCT('a', 'b')))::ARRAY(VARIANT)", "{\"a\": \"b\"}")]
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<string>(0);

                    // assert
                    Assert.NotNull(array);
                    CollectionAssert.AreEqual(new[] { RemoveWhiteSpaces(expectedValue) }, array.Select(RemoveWhiteSpaces).ToArray());
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
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
        public void TestSelectArrayOfDoublesWithExponentNotation()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arrayOfDoubles = "ARRAY_CONSTRUCT(1.0e100, 1.0e-100)::ARRAY(DOUBLE)";
                    command.CommandText = $"SELECT {arrayOfDoubles}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var array = reader.GetArray<double>(0);

                    // assert
                    Assert.AreEqual(2, array.Length);
                    CollectionAssert.AreEqual(new[] { 1.0e100d, 1.0e-100d }, array);
                }
            }
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
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when getting an array"));
                    Assert.That(thrown.Message, Does.Contain("Method GetArray<System.String> can be used only for structured array"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidArrayElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = "ARRAY_CONSTRUCT('a76dacad-0e35-497b-bf9b-7cd49262b68b', 'z76dacad-0e35-497b-bf9b-7cd49262b68b')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<Guid>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when reading path $[1]"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForNextedInvalidElement()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var arraySFString = @"ARRAY_CONSTRUCT(
                        OBJECT_CONSTRUCT('x', 'a', 'y', 'b')
                    )::ARRAY(OBJECT(x VARCHAR, y VARCHAR))";
                    command.CommandText = $"SELECT {arraySFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => reader.GetArray<AnnotatedClassForConstructorConstruction>(0));

                    // assert
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR);
                    Assert.That(thrown.Message, Does.Contain("Failed to read structured type when reading path $[0][1]"));
                    Assert.That(thrown.Message, Does.Contain("Could not read text type into System.Int32"));
                }
            }
        }

    }
}
