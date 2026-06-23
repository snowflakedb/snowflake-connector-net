using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using System.Data.Common;
using Snowflake.Data.Core;
using System;
using System.Threading;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class VectorTypesITJson : VectorTypesIT
    {
        public VectorTypesITJson(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.JSON) { }
    }

    public sealed class VectorTypesITArrow : VectorTypesIT
    {
        public VectorTypesITArrow(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW) { }
    }

    public abstract class VectorTypesIT : SFBaseTestAsync
    {
        private readonly ResultFormat _resultFormat;

        private readonly SFBaseTestAsyncFixture _fixture;
        public VectorTypesIT(SFBaseTestAsyncFixture fixture, ResultFormat resultFormat) : base(fixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
        }

        [SFFact]
        public async Task TestSelectIntVectorFromTable()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"CREATE OR REPLACE TABLE {tableName} (a VECTOR(INT, 3));";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [1,2,3]::VECTOR(INT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [4,5,6]::VECTOR(INT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [7,8,9]::VECTOR(INT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                    command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);
                    Assert.True(reader.Read());
                    Assert.Equal(3, reader.GetInt16(0));

                    command.CommandText = $"SELECT * FROM {tableName};";
                    reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1,2,3]", reader.GetString(0));
                    var arr = reader.GetArray<int>(0);
                    Assert.Equal(1, arr[0]);
                    Assert.Equal(2, arr[1]);
                    Assert.Equal(3, arr[2]);

                    Assert.True(reader.Read());
                    Assert.Equal("[4,5,6]", reader.GetString(0));
                    arr = reader.GetArray<int>(0);
                    Assert.Equal(4, arr[0]);
                    Assert.Equal(5, arr[1]);
                    Assert.Equal(6, arr[2]);

                    Assert.True(reader.Read());
                    Assert.Equal("[7,8,9]", reader.GetString(0));
                    arr = reader.GetArray<int>(0);
                    Assert.Equal(7, arr[0]);
                    Assert.Equal(8, arr[1]);
                    Assert.Equal(9, arr[2]);

                    command.CommandText = $"DROP TABLE IF EXISTS {tableName};";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        [SFFact]
        public async Task TestSelectFloatVectorFromTable()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"CREATE OR REPLACE TABLE {tableName} (a VECTOR(FLOAT, 3));";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [1.1,2.2,3.3]::VECTOR(FLOAT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [4.4,5.5,6.6]::VECTOR(FLOAT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    command.CommandText = $"INSERT INTO {tableName} SELECT [7.7,8.8,9.9]::VECTOR(FLOAT,3);";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                    command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);
                    Assert.True(reader.Read());
                    Assert.Equal(3, reader.GetInt16(0));

                    command.CommandText = $"SELECT * FROM {tableName};";
                    reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1.100000,2.200000,3.300000]", reader.GetString(0));
                    var arr = reader.GetArray<float>(0);
                    Assert.Equal(1.1f, arr[0]);
                    Assert.Equal(2.2f, arr[1]);
                    Assert.Equal(3.3f, arr[2]);

                    Assert.True(reader.Read());
                    Assert.Equal("[4.400000,5.500000,6.600000]", reader.GetString(0));
                    arr = reader.GetArray<float>(0);
                    Assert.Equal(4.4f, arr[0]);
                    Assert.Equal(5.5f, arr[1]);
                    Assert.Equal(6.6f, arr[2]);

                    Assert.True(reader.Read());
                    Assert.Equal("[7.700000,8.800000,9.900000]", reader.GetString(0));
                    arr = reader.GetArray<float>(0);
                    Assert.Equal(7.7f, arr[0]);
                    Assert.Equal(8.8f, arr[1]);
                    Assert.Equal(9.9f, arr[2]);

                    command.CommandText = $"DROP TABLE IF EXISTS {tableName};";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        [SFFact]
        public async Task TestSelectIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1, 2, 3]::VECTOR(INT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1,2,3]", reader.GetString(0));

                    var arr = reader.GetArray<int>(0);
                    Assert.Equal(1, arr[0]);
                    Assert.Equal(2, arr[1]);
                    Assert.Equal(3, arr[2]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectIntVectorWithMinAndMax32BitValues()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT [{Int32.MinValue}, {Int32.MaxValue}]::VECTOR(INT, 2) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal($"[{Int32.MinValue},{Int32.MaxValue}]", reader.GetString(0));

                    var arr = reader.GetArray<int>(0);
                    Assert.Equal(Int32.MinValue, arr[0]);
                    Assert.Equal(Int32.MaxValue, arr[1]);
                }
            }
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestThrowExceptionForInvalidValueForIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.1]::VECTOR(INT, 3) as vec;";

                    var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () => await command.ExecuteReaderAsync().ConfigureAwait(false)).ConfigureAwait(false);

                    AssertExtensions.AnySucceeds(
                        () => Assert.Contains("Array-like value being cast to a vector has incorrect dimension", thrown.Message),
                        () => Assert.Contains("Vector value being cast to a vector is not an array or vector, or has incorrect dimension or element type", thrown.Message));
                }
            }
        }

        [SFFact]
        public async Task TestThrowExceptionForInvalidIdentifierForIntVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [A, B, C]::VECTOR(INT, 3) as vec;";

                    var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () => await command.ExecuteReaderAsync().ConfigureAwait(false)).ConfigureAwait(false);

                    Assert.Contains("invalid identifier", thrown.Message);
                }
            }
        }

        [SFFact]
        public async Task TestSelectFloatVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.1,2.22,3.333]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1.100000,2.220000,3.333000]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.Equal(1.1f, arr[0]);
                    Assert.Equal(2.22f, arr[1]);
                    Assert.Equal(3.333f, arr[2]);
                }
            }
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSelectFloatVectorWithMinAndMaxFloatValues()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT [{float.MinValue}, {float.MaxValue}]::VECTOR(FLOAT, 2) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());

                    var arr = reader.GetArray<float>(0);
#if NETFRAMEWORK
                    Assert.Equal(float.MinValue.ToString(), arr[0].ToString());
                    Assert.Equal(float.MaxValue.ToString(), arr[1].ToString());
#else
                    Assert.Equal(float.MinValue, arr[0]);
                    Assert.Equal(float.MaxValue, arr[1]);
#endif
                }
            }
        }

        [SFFact]
        public async Task TestSelectFloatVectorWithNoDecimals()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1,2,3]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1.000000,2.000000,3.000000]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.Equal(1f, arr[0]);
                    Assert.Equal(2f, arr[1]);
                    Assert.Equal(3f, arr[2]);
                }
            }
        }

        [SFFact]
        public async Task TestSelectFloatVectorWithGreaterThanSixDigitPrecision()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [1.123456789,2.123456789,3.123456789]::VECTOR(FLOAT, 3) as vec;";
                    var reader = (SnowflakeDbDataReader)await command.ExecuteReaderAsync().ConfigureAwait(false);

                    Assert.True(reader.Read());
                    Assert.Equal("[1.123457,2.123457,3.123457]", reader.GetString(0));

                    var arr = reader.GetArray<float>(0);
                    Assert.Equal(1.123457f, arr[0]);
                    Assert.Equal(2.123457f, arr[1]);
                    Assert.Equal(3.123457f, arr[2]);
                }
            }
        }

        [SFFact]
        public async Task TestThrowExceptionForInvalidIdentifierForFloatVector()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT [A, B, C]::VECTOR(FLOAT, 3) as vec;";

                    var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () => await command.ExecuteReaderAsync().ConfigureAwait(false)).ConfigureAwait(false);

                    Assert.Contains("invalid identifier", thrown.Message);
                }
            }
        }

        private async Task AlterSessionSettingsAsync(DbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {_resultFormat}";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
