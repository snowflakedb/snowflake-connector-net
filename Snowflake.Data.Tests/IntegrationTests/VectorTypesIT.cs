using NUnit.Framework;
using Snowflake.Data.Client;
using System.Data.Common;
using Snowflake.Data.Core;
using System;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    class VectorTypesIT : SFBaseTest
    {
        private readonly ResultFormat _resultFormat;

        public VectorTypesIT(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        [Test]
        public void TestSelectIntVectorFromTable()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"CREATE OR REPLACE TABLE {TableName} (a VECTOR(INT, 3));";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [1,2,3]::VECTOR(INT,3);";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [4,5,6]::VECTOR(INT,3);";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [7,8,9]::VECTOR(INT,3);";
                    command.ExecuteNonQuery();

                    command.CommandText = $"SELECT COUNT(*) FROM {TableName};";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt16(0));

                    command.CommandText = $"SELECT * FROM {TableName};";
                    reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1,2,3]", reader.GetString(0));
                    var arr = reader.GetArray<int>(0);
                    Assert.AreEqual(1, arr[0]);
                    Assert.AreEqual(2, arr[1]);
                    Assert.AreEqual(3, arr[2]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[4,5,6]", reader.GetString(0));
                    arr = reader.GetArray<int>(0);
                    Assert.AreEqual(4, arr[0]);
                    Assert.AreEqual(5, arr[1]);
                    Assert.AreEqual(6, arr[2]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[7,8,9]", reader.GetString(0));
                    arr = reader.GetArray<int>(0);
                    Assert.AreEqual(7, arr[0]);
                    Assert.AreEqual(8, arr[1]);
                    Assert.AreEqual(9, arr[2]);

                    command.CommandText = $"DROP TABLE IF EXISTS {TableName};";
                    command.ExecuteNonQuery();
                }
            }
        }

        [Test]
        public void TestSelectFloatVectorFromTable()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"CREATE OR REPLACE TABLE {TableName} (a VECTOR(FLOAT, 3));";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [1.1,2.2,3.3]::VECTOR(FLOAT,3);";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [4.4,5.5,6.6]::VECTOR(FLOAT,3);";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {TableName} SELECT [7.7,8.8,9.9]::VECTOR(FLOAT,3);";
                    command.ExecuteNonQuery();

                    command.CommandText = $"SELECT COUNT(*) FROM {TableName};";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt16(0));

                    command.CommandText = $"SELECT * FROM {TableName};";
                    reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1.100000,2.200000,3.300000]", reader.GetString(0));
                    var arr = reader.GetArray<float>(0);
                    Assert.AreEqual(1.1f, arr[0]);
                    Assert.AreEqual(2.2f, arr[1]);
                    Assert.AreEqual(3.3f, arr[2]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[4.400000,5.500000,6.600000]", reader.GetString(0));
                    arr = reader.GetArray<float>(0);
                    Assert.AreEqual(4.4f, arr[0]);
                    Assert.AreEqual(5.5f, arr[1]);
                    Assert.AreEqual(6.6f, arr[2]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[7.700000,8.800000,9.900000]", reader.GetString(0));
                    arr = reader.GetArray<float>(0);
                    Assert.AreEqual(7.7f, arr[0]);
                    Assert.AreEqual(8.8f, arr[1]);
                    Assert.AreEqual(9.9f, arr[2]);

                    command.CommandText = $"DROP TABLE IF EXISTS {TableName};";
                    command.ExecuteNonQuery();
                }
            }
        }

        [Test]
        public void TestSelectIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1, 2, 3]::VECTOR(INT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1,2,3]", reader.GetString(0));

                    var arr = reader.GetArray<int>(0);
                    Assert.AreEqual(1, arr[0]);
                    Assert.AreEqual(2, arr[1]);
                    Assert.AreEqual(3, arr[2]);
                }
            }
        }

        [Test]
        public void TestSelectIntVectorWithMinAndMax32BitValues()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT [{Int32.MinValue}, {Int32.MaxValue}]::VECTOR(INT, 2) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual($"[{Int32.MinValue},{Int32.MaxValue}]", reader.GetString(0));

                    var arr = reader.GetArray<int>(0);
                    Assert.AreEqual(Int32.MinValue, arr[0]);
                    Assert.AreEqual(Int32.MaxValue, arr[1]);
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidValueForIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.1]::VECTOR(INT, 3) as vec;";

                    var thrown = Assert.Throws<SnowflakeDbException>(() => command.ExecuteReader());

                    Assert.That(thrown.Message, Does.Contain("Array-like value being cast to a vector has incorrect dimension"));
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidIdentifierForIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [A, B, C]::VECTOR(INT, 3) as vec;";

                    var thrown = Assert.Throws<SnowflakeDbException>(() => command.ExecuteReader());

                    Assert.That(thrown.Message, Does.Contain("invalid identifier"));
                }
            }
        }

        [Test]
        public void TestSelectFloatVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.1,2.22,3.333]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1.100000,2.220000,3.333000]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.AreEqual(1.1f, arr[0]);
                    Assert.AreEqual(2.22f, arr[1]);
                    Assert.AreEqual(3.333f, arr[2]);
                }
            }
        }

        [Test]
        public void TestSelectFloatVectorWithMinAndMaxFloatValues()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT [{float.MinValue}, {float.MaxValue}]::VECTOR(FLOAT, 2) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());

                    var arr = reader.GetArray<float>(0);
#if NETFRAMEWORK
                    Assert.AreEqual(float.MinValue.ToString(), arr[0].ToString());
                    Assert.AreEqual(float.MaxValue.ToString(), arr[1].ToString());
#else
                    Assert.AreEqual(float.MinValue, arr[0]);
                    Assert.AreEqual(float.MaxValue, arr[1]);
#endif
                }
            }
        }

        [Test]
        public void TestSelectFloatVectorWithNoDecimals()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1,2,3]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1.000000,2.000000,3.000000]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.AreEqual(1f, arr[0]);
                    Assert.AreEqual(2f, arr[1]);
                    Assert.AreEqual(3f, arr[2]);
                }
            }
        }

        [Test]
        public void TestSelectFloatVectorWithGreaterThanSixDigitPrecision()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.123456789,2.123456789,3.123456789]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("[1.123457,2.123457,3.123457]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.AreEqual(1.123457f, arr[0]);
                    Assert.AreEqual(2.123457f, arr[1]);
                    Assert.AreEqual(3.123457f, arr[2]);
                }
            }
        }

        [Test]
        public void TestThrowExceptionForInvalidIdentifierForFloatVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                AlterSessionSettings(conn);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [A, B, C]::VECTOR(FLOAT, 3) as vec;";

                    var thrown = Assert.Throws<SnowflakeDbException>(() => command.ExecuteReader());

                    Assert.That(thrown.Message, Does.Contain("invalid identifier"));
                }
            }
        }

        private void AlterSessionSettings(DbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {_resultFormat}";
                command.ExecuteNonQuery();
            }
        }
    }
}
