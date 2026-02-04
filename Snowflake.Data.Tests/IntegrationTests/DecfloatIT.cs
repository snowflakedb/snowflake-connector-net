using System;
using System.Data;
using System.Globalization;
using System.Reflection;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    /// <summary>
    /// Integration tests for DECFLOAT data type support.
    /// DECFLOAT values are returned as strings to preserve full precision.
    /// Arrow format uses scientific notation; JSON format uses backend's format.
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

        private SnowflakeDbConnection CreateAndOpenConnection()
        {
            var conn = new SnowflakeDbConnection(ConnectionString);
            conn.Open();
            return conn;
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

        /// <summary>
        /// Parses a DECFLOAT string value (may be scientific notation) to decimal for comparison.
        /// </summary>
        private static decimal ParseDecfloatValue(string value)
        {
            return decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
        }

        [Test]
        public void TestSelectDecfloatLiteral()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.AreEqual(typeof(string), reader.GetFieldType(0));

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);

                        // Parse and compare numerically (format may vary between Arrow/JSON)
                        Assert.AreEqual(123.456m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatHighPrecision()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1234567890.123456789::DECFLOAT AS high_precision";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);
                        Assert.AreEqual(1234567890.123456789m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNull()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT NULL::DECFLOAT AS null_decfloat";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());
                        Assert.IsTrue(reader.IsDBNull(0));
                        Assert.AreEqual(DBNull.Value, reader.GetValue(0));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNegative()
        {
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

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);
                        Assert.AreEqual(-987.654m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatZero()
        {
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

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);
                        Assert.AreEqual(0m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatLargePositiveExponent()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.23e10::DECFLOAT AS large_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);
                        Assert.AreEqual(12300000000m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatSmallNegativeExponent()
        {
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

                        var value = reader.GetValue(0);
                        Assert.IsInstanceOf<string>(value);
                        Assert.AreEqual(0.0015m, ParseDecfloatValue((string)value));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatInTable()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                CreateOrReplaceTable(conn, TableName, new[] { "col_decfloat DECFLOAT" });

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {TableName} VALUES (123.456), (-999.999), (NULL)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT col_decfloat FROM {TableName} ORDER BY col_decfloat NULLS LAST";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(0));

                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual(-999.999m, ParseDecfloatValue((string)reader.GetValue(0)));

                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual(123.456m, ParseDecfloatValue((string)reader.GetValue(0)));

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
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT 
                        1.1::DECFLOAT AS col1,
                        2.2::DECFLOAT AS col2,
                        3.3::DECFLOAT AS col3";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Assert.AreEqual(3, reader.FieldCount);
                        for (int i = 0; i < 3; i++)
                        {
                            Assert.AreEqual("DECFLOAT", reader.GetDataTypeName(i));
                            Assert.AreEqual(typeof(string), reader.GetFieldType(i));
                        }

                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual(1.1m, ParseDecfloatValue((string)reader.GetValue(0)));
                        Assert.AreEqual(2.2m, ParseDecfloatValue((string)reader.GetValue(1)));
                        Assert.AreEqual(3.3m, ParseDecfloatValue((string)reader.GetValue(2)));

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        [Explicit("Diagnostic test to observe backend response with current driver version")]
        public void TestDecfloatWithCurrentDriverVersion()
        {
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        var dataTypeName = reader.GetDataTypeName(0);
                        var fieldType = reader.GetFieldType(0);

                        Console.WriteLine($"Result format: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {dataTypeName}");
                        Console.WriteLine($"FieldType: {fieldType}");

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Console.WriteLine($"Value type: {value?.GetType()?.Name ?? "null"}");
                        Console.WriteLine($"Value: {value}");

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        [Explicit("Diagnostic test to observe backend response with older driver version")]
        public void TestDecfloatWithOlderDriverVersion()
        {
            // Get original version
            var versionProp = typeof(SFEnvironment).GetProperty("DriverVersion", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
            var originalVersion = (string)versionProp.GetValue(null);

            try
            {
                // Set older version (pre-DECFLOAT support)
                versionProp.SetValue(null, "4.0.0");
                Console.WriteLine($"Driver version set to: {versionProp.GetValue(null)}");

                using (var conn = CreateAndOpenConnection())
                {
                    SetResultFormat(conn);

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                        using (var reader = cmd.ExecuteReader())
                        {
                            ValidateResultFormat(reader);

                            var dataTypeName = reader.GetDataTypeName(0);
                            var fieldType = reader.GetFieldType(0);

                            Console.WriteLine($"Result format: {_resultFormat}");
                            Console.WriteLine($"DataTypeName: {dataTypeName}");
                            Console.WriteLine($"FieldType: {fieldType}");

                            Assert.IsTrue(reader.Read());

                            var value = reader.GetValue(0);
                            Console.WriteLine($"Value type: {value?.GetType()?.Name ?? "null"}");
                            Console.WriteLine($"Value: {value}");

                            Assert.IsFalse(reader.Read());
                        }
                    }
                }
            }
            finally
            {
                // Restore original version
                versionProp.SetValue(null, originalVersion);
            }
        }
    }
}
