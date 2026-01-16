using System;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    /// <summary>
    /// Integration tests for DECFLOAT data type support.
    /// </summary>
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    class DecfloatIT : SFBaseTest
    {
        protected override string TestName => base.TestName + _resultFormat;

        private readonly ResultFormat _resultFormat;

        public DecfloatIT(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        private void SetResultFormat(IDbConnection conn)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = '{_resultFormat}'";
                cmd.ExecuteNonQuery();
            }
        }

        private void ValidateResultFormat(IDataReader reader)
        {
            Assert.AreEqual(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);
        }

        [Test]
        public void TestSelectDecfloatLiteral()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert metadata
                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));

                        Assert.IsTrue(reader.Read());

                        // Assert value
                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<decimal>(value);
                        Assert.AreEqual(123.456m, (decimal)value);

                        // Assert type accessors work
                        Assert.AreEqual(123.456m, reader.GetDecimal(0));
                        Assert.AreEqual(123.456d, reader.GetDouble(0), 0.0001);
                        Assert.AreEqual("123.456", reader.GetString(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatWithHighPrecision()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    // Use a value within decimal precision (28-29 digits)
                    cmd.CommandText = "SELECT 1234567890.12345678901234567890::DECFLOAT AS high_precision";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert metadata
                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));

                        Assert.IsTrue(reader.Read());

                        // Assert value is readable and is decimal type
                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<decimal>(value);
                        Assert.IsNotNull(value);

                        // Verify it starts with the expected integer part
                        var decimalValue = (decimal)value;
                        Assert.That(decimalValue, Is.GreaterThan(1234567890m));
                        Assert.That(decimalValue, Is.LessThan(1234567891m));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatWithScientificNotation()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.23e10::DECFLOAT AS scientific_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert metadata
                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));

                        Assert.IsTrue(reader.Read());

                        // Assert value (1.23e10 = 12300000000)
                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<decimal>(value);
                        Assert.AreEqual(12300000000m, (decimal)value);

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNegativeScientificNotation()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.5e-3::DECFLOAT AS small_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());

                        // Assert value (1.5e-3 = 0.0015)
                        var value = reader.GetDecimal(0);
                        Assert.AreEqual(0.0015m, value);

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNullValue()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT NULL::DECFLOAT AS null_decfloat";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert metadata
                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));

                        Assert.IsTrue(reader.Read());

                        // Assert NULL handling
                        Assert.IsTrue(reader.IsDBNull(0));
                        Assert.AreEqual(DBNull.Value, reader.GetValue(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatZeroValue()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 0::DECFLOAT AS zero_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());

                        // Assert zero value
                        Assert.IsFalse(reader.IsDBNull(0));
                        Assert.AreEqual(0m, reader.GetDecimal(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNegativeValue()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT -987.654::DECFLOAT AS negative_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());

                        // Assert negative value
                        Assert.AreEqual(-987.654m, reader.GetDecimal(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatInTable()
        {
            // Arrange
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                CreateOrReplaceTable(conn, TableName, new[] { "col_decfloat DECFLOAT" });

                using (var cmd = conn.CreateCommand())
                {
                    // Insert values
                    cmd.CommandText = $"INSERT INTO {TableName} VALUES (123.456), (1e10), (-999.999), (NULL)";
                    var inserted = cmd.ExecuteNonQuery();
                    Assert.AreEqual(4, inserted);

                    // Act - Query the data
                    cmd.CommandText = $"SELECT col_decfloat FROM {TableName} ORDER BY col_decfloat NULLS LAST";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert metadata
                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.AreEqual(typeof(decimal), reader.GetFieldType(0));

                        // Row 1: -999.999
                        Assert.IsTrue(reader.Read());
                        Assert.IsFalse(reader.IsDBNull(0));
                        Assert.AreEqual(-999.999m, reader.GetDecimal(0));

                        // Row 2: 123.456
                        Assert.IsTrue(reader.Read());
                        Assert.IsFalse(reader.IsDBNull(0));
                        Assert.AreEqual(123.456m, reader.GetDecimal(0));

                        // Row 3: 1e10 = 10000000000
                        Assert.IsTrue(reader.Read());
                        Assert.IsFalse(reader.IsDBNull(0));
                        Assert.AreEqual(10000000000m, reader.GetDecimal(0));

                        // Row 4: NULL
                        Assert.IsTrue(reader.Read());
                        Assert.IsTrue(reader.IsDBNull(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatMultipleColumns()
        {
            // Arrange & Act
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"
                        SELECT 
                            1.1::DECFLOAT AS col1,
                            2.2::DECFLOAT AS col2,
                            3.3::DECFLOAT AS col3
                    ";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Assert all columns have correct metadata
                        Assert.AreEqual(3, reader.FieldCount);
                        for (int i = 0; i < 3; i++)
                        {
                            Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(i));
                            Assert.AreEqual(typeof(decimal), reader.GetFieldType(i));
                        }

                        Assert.IsTrue(reader.Read());

                        // Assert values
                        Assert.AreEqual(1.1m, reader.GetDecimal(0));
                        Assert.AreEqual(2.2m, reader.GetDecimal(1));
                        Assert.AreEqual(3.3m, reader.GetDecimal(2));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        private SnowflakeDbConnection CreateAndOpenConnection()
        {
            var conn = new SnowflakeDbConnection(ConnectionString);
            conn.Open();
            return conn;
        }
    }
}
