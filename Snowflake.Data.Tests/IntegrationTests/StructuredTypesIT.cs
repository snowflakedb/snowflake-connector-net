using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class StructuredTypesIT : SFBaseTest
    {
        private static string _tableName = "structured_types_tests";

        [Test]
        public void TestInsertStructuredTypeObject()
        {
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                // arrange
                connection.Open();
                CreateOrReplaceTable(connection, _tableName, new List<string> { "address OBJECT(city VARCHAR, state VARCHAR)" });
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var addressAsSFString = "OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)";
                    command.CommandText = $"INSERT INTO {_tableName} SELECT {addressAsSFString}";
                    command.ExecuteNonQuery();
                    command.CommandText = $"SELECT * FROM {_tableName}";

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
        [TestCase(StructureTypeConstructionMethod.PROPERTIES_NAMES)]
        [TestCase(StructureTypeConstructionMethod.PROPERTIES_ORDER)]
        [TestCase(StructureTypeConstructionMethod.CONSTRUCTOR)]
        public void TestSelectNestedStructuredTypeObject(StructureTypeConstructionMethod constructionMethod)
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
                    var address = reader.GetObject<Address>(0, constructionMethod);
                    Assert.AreEqual("San Mateo", address.city);
                    Assert.AreEqual("CA", address.state);
                    Assert.NotNull(address.zip);
                    Assert.AreEqual("00", address.zip.prefix);
                    Assert.AreEqual("11", address.zip.postfix);
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
                    var arrayOfNumberSFString = "ARRAY_CONSTRUCT('a','b','c')::ARRAY(TEXT)";
                    command.CommandText = $"SELECT {arrayOfNumberSFString}";

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
                    var arrayOfObjects = "ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b'), ARRAY_CONSTRUCT('c', 'd'))::ARRAY(ARRAY(TEXT))";
                    command.CommandText = $"SELECT {arrayOfObjects}";

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

        private void EnableStructuredTypes(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                // command.CommandText = "ALTER SESSION SET FEATURE_STRUCTURED_TYPES = enabled";
                // command.ExecuteNonQuery();
                // command.CommandText = "ALTER SESSION SET ENABLE_STRUCTURED_TYPES_IN_FDN_TABLES = true";
                // command.ExecuteNonQuery();
                command.CommandText = "ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT=JSON";
                command.ExecuteNonQuery();
            }
        }
    }
}
