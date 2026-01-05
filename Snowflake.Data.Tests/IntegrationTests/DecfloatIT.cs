using System;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    /// <summary>
    /// Integration tests for DECFLOAT data type support.
    /// These tests explore how DECFLOAT values are handled by the connector.
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
            // Test selecting a DECFLOAT literal value
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    // Select a DECFLOAT value using explicit cast
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        // Check metadata
                        var schemaTable = reader.GetSchemaTable();
                        var dataTypeName = reader.GetDataTypeName(0);
                        var fieldType = reader.GetFieldType(0);

                        Console.WriteLine($"ResultFormat: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {dataTypeName}");
                        Console.WriteLine($"FieldType: {fieldType}");

                        Assert.IsTrue(reader.Read());

                        // Try to read the value
                        var value = reader.GetValue(0);
                        Console.WriteLine($"Value: {value}");
                        Console.WriteLine($"Value Type: {value?.GetType()}");

                        // Try specific type accessors
                        try
                        {
                            var decimalValue = reader.GetDecimal(0);
                            Console.WriteLine($"GetDecimal: {decimalValue}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"GetDecimal failed: {ex.Message}");
                        }

                        try
                        {
                            var doubleValue = reader.GetDouble(0);
                            Console.WriteLine($"GetDouble: {doubleValue}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"GetDouble failed: {ex.Message}");
                        }

                        try
                        {
                            var stringValue = reader.GetString(0);
                            Console.WriteLine($"GetString: {stringValue}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"GetString failed: {ex.Message}");
                        }

                        Assert.IsFalse(reader.Read());
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatWithHighPrecision()
        {
            // Test DECFLOAT with high precision values
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    // DECFLOAT can handle very high precision numbers
                    cmd.CommandText = "SELECT 1234567890.123456789012345678901234567890::DECFLOAT AS high_precision";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        var dataTypeName = reader.GetDataTypeName(0);
                        Console.WriteLine($"ResultFormat: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {dataTypeName}");

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Console.WriteLine($"High precision value: {value}");
                        Console.WriteLine($"Value Type: {value?.GetType()}");
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatWithScientificNotation()
        {
            // Test DECFLOAT with scientific notation
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.23e10::DECFLOAT AS scientific_value";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Console.WriteLine($"ResultFormat: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {reader.GetDataTypeName(0)}");

                        Assert.IsTrue(reader.Read());

                        var value = reader.GetValue(0);
                        Console.WriteLine($"Scientific notation value: {value}");
                        Console.WriteLine($"Value Type: {value?.GetType()}");
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatNullValue()
        {
            // Test NULL DECFLOAT value
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT NULL::DECFLOAT AS null_decfloat";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Console.WriteLine($"ResultFormat: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {reader.GetDataTypeName(0)}");

                        Assert.IsTrue(reader.Read());

                        var isNull = reader.IsDBNull(0);
                        Console.WriteLine($"IsDBNull: {isNull}");

                        var value = reader.GetValue(0);
                        Console.WriteLine($"Value: {value}");
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatInTable()
        {
            // Test DECFLOAT column in a table
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                // Create table with DECFLOAT column
                CreateOrReplaceTable(conn, TableName, new[] { "col_decfloat DECFLOAT" });

                using (var cmd = conn.CreateCommand())
                {
                    // Insert values
                    cmd.CommandText = $"INSERT INTO {TableName} VALUES (123.456), (1e20), (-999.999), (NULL)";
                    var inserted = cmd.ExecuteNonQuery();
                    Console.WriteLine($"Inserted {inserted} rows");

                    // Query the data
                    cmd.CommandText = $"SELECT col_decfloat FROM {TableName} ORDER BY col_decfloat NULLS LAST";

                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        Console.WriteLine($"ResultFormat: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {reader.GetDataTypeName(0)}");
                        Console.WriteLine($"FieldType: {reader.GetFieldType(0)}");

                        int rowNum = 0;
                        while (reader.Read())
                        {
                            rowNum++;
                            var isNull = reader.IsDBNull(0);
                            var value = reader.GetValue(0);
                            Console.WriteLine($"Row {rowNum}: IsNull={isNull}, Value={value}, Type={value?.GetType()}");
                        }
                    }
                }
            }
        }

        [Test]
        public void TestDecfloatSpecialValues()
        {
            // Test special DECFLOAT values like infinity and NaN if supported
            using (var conn = CreateAndOpenConnection())
            {
                SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    // Test various special values
                    cmd.CommandText = @"
                        SELECT 
                            0::DECFLOAT AS zero,
                            -0::DECFLOAT AS negative_zero,
                            'inf'::DECFLOAT AS positive_infinity,
                            '-inf'::DECFLOAT AS negative_infinity,
                            'nan'::DECFLOAT AS not_a_number
                    ";

                    try
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            ValidateResultFormat(reader);

                            Console.WriteLine($"ResultFormat: {_resultFormat}");

                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.WriteLine($"Column {i}: {reader.GetName(i)}, Type: {reader.GetDataTypeName(i)}");
                            }

                            Assert.IsTrue(reader.Read());

                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var value = reader.GetValue(i);
                                Console.WriteLine($"{reader.GetName(i)}: {value} (Type: {value?.GetType()})");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Special values test failed: {ex.Message}");
                        // This might fail if DECFLOAT doesn't support special values like 'inf' or 'nan'
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

        private void CloseConnection(SnowflakeDbConnection conn)
        {
            conn.Close();
        }
    }
}

