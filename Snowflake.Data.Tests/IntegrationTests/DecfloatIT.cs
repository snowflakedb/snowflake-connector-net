using System;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class DecfloatITJson : DecfloatIT
    {
        public DecfloatITJson(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture, ResultFormat.JSON)
        {
        }
    }

    public sealed class DecfloatITArrow : DecfloatIT
    {
        public DecfloatITArrow(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture, ResultFormat.ARROW)
        {
        }
    }

    /// <summary>
    /// Integration tests for DECFLOAT data type support.
    /// DECFLOAT values are returned as strings to preserve full precision.
    /// Arrow format uses scientific notation; JSON format uses backend's format.
    /// </summary>
    public abstract class DecfloatIT : SFBaseTestAsync
    {
        private readonly ResultFormat _resultFormat;

        private readonly SFBaseTestAsyncFixture _fixture;
        public DecfloatIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture, ResultFormat resultFormat) : base(fixture, envFixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
        }

        private async Task<SnowflakeDbConnection> CreateAndOpenConnectionAsync()
        {
            var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn.OpenAsync(CancellationToken.None);
            return conn;
        }

        private async Task SetResultFormat(IDbConnection conn)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = '{_resultFormat}'";
                cmd.ExecuteNonQuery();
            }
        }

        private void ValidateResultFormat(IDataReader reader)
        {
            Assert.Equal(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);
        }

        /// <summary>
        /// Parses a DECFLOAT string value (may be scientific notation) to decimal for comparison.
        /// </summary>
        private static decimal ParseDecfloatValue(string value)
        {
            return decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
        }

        [Fact]
        public async Task TestSelectDecfloatLiteral()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.Equal("DECFLOAT", reader.GetDataTypeName(0));
                        Assert.Equal(typeof(string), reader.GetFieldType(0));

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);

                        // Parse and compare numerically (format may vary between Arrow/JSON)
                        Assert.Equal(123.456m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatHighPrecision()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1234567890.123456789::DECFLOAT AS high_precision";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);
                        Assert.Equal(1234567890.123456789m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatNull()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT NULL::DECFLOAT AS null_decfloat";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());
                        Assert.True(reader.IsDBNull(0));
                        Assert.Equal(DBNull.Value, reader.GetValue(0));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatNegative()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT -987.654::DECFLOAT AS negative_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);
                        Assert.Equal(-987.654m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatZero()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 0::DECFLOAT AS zero_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);
                        Assert.Equal(0m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatLargePositiveExponent()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.23e10::DECFLOAT AS large_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);
                        Assert.Equal(12300000000m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatSmallNegativeExponent()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1.5e-3::DECFLOAT AS small_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Assert.IsType<string>(value);
                        Assert.Equal(0.0015m, ParseDecfloatValue((string)value));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatInTable()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                _fixture.CreateOrReplaceTable(conn, _fixture.TableName, new[] { "col_decfloat DECFLOAT" });

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {_fixture.TableName} VALUES (123.456), (-999.999), (NULL)";
                    await cmd.ExecuteNonQueryAsync();

                    cmd.CommandText = $"SELECT col_decfloat FROM {_fixture.TableName} ORDER BY col_decfloat NULLS LAST";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.Equal("DECFLOAT", reader.GetDataTypeName(0));

                        Assert.True(reader.Read());
                        Assert.Equal(-999.999m, ParseDecfloatValue((string)reader.GetValue(0)));

                        Assert.True(reader.Read());
                        Assert.Equal(123.456m, ParseDecfloatValue((string)reader.GetValue(0)));

                        Assert.True(reader.Read());
                        Assert.True(reader.IsDBNull(0));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatMultipleColumns()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT
                        1.1::DECFLOAT AS col1,
                        2.2::DECFLOAT AS col2,
                        3.3::DECFLOAT AS col3";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        Assert.Equal(3, reader.FieldCount);
                        for (int i = 0; i < 3; i++)
                        {
                            Assert.Equal("DECFLOAT", reader.GetDataTypeName(i));
                            Assert.Equal(typeof(string), reader.GetFieldType(i));
                        }

                        Assert.True(reader.Read());
                        Assert.Equal(1.1m, ParseDecfloatValue((string)reader.GetValue(0)));
                        Assert.Equal(2.2m, ParseDecfloatValue((string)reader.GetValue(1)));
                        Assert.Equal(3.3m, ParseDecfloatValue((string)reader.GetValue(2)));

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatWithCurrentDriverVersion()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await SetResultFormat(conn);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        ValidateResultFormat(reader);

                        var dataTypeName = reader.GetDataTypeName(0);
                        var fieldType = reader.GetFieldType(0);

                        Console.WriteLine($"Result format: {_resultFormat}");
                        Console.WriteLine($"DataTypeName: {dataTypeName}");
                        Console.WriteLine($"FieldType: {fieldType}");

                        Assert.True(reader.Read());

                        var value = reader.GetValue(0);
                        Console.WriteLine($"Value type: {value?.GetType()?.Name ?? "null"}");
                        Console.WriteLine($"Value: {value}");

                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Fact]
        public async Task TestDecfloatWithOlderDriverVersion()
        {
            // Get original version
            var versionProp = typeof(SFEnvironment).GetProperty("DriverVersion", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
            var originalVersion = (string)versionProp.GetValue(null);

            try
            {
                // Set older version (pre-DECFLOAT support)
                versionProp.SetValue(null, "4.0.0");
                Console.WriteLine($"Driver version set to: {versionProp.GetValue(null)}");

                using (var conn = await CreateAndOpenConnectionAsync())
                {
                    await SetResultFormat(conn);

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT 123.456::DECFLOAT AS decfloat_value";

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            ValidateResultFormat(reader);

                            var dataTypeName = reader.GetDataTypeName(0);
                            var fieldType = reader.GetFieldType(0);

                            Console.WriteLine($"Result format: {_resultFormat}");
                            Console.WriteLine($"DataTypeName: {dataTypeName}");
                            Console.WriteLine($"FieldType: {fieldType}");

                            Assert.True(reader.Read());

                            var value = reader.GetValue(0);
                            Console.WriteLine($"Value type: {value?.GetType()?.Name ?? "null"}");
                            Console.WriteLine($"Value: {value}");

                            Assert.False(reader.Read());
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
