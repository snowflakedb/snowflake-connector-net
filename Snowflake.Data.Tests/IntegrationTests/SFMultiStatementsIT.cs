using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFMultiStatementsITJson : SFMultiStatementsIT
    {
        public SFMultiStatementsITJson(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.JSON) { }
    }

    public sealed class SFMultiStatementsITArrow : SFMultiStatementsIT
    {
        public SFMultiStatementsITArrow(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW) { }
    }

    public abstract class SFMultiStatementsIT : SFBaseTestAsync
    {
        private readonly ResultFormat _resultFormat;

        private readonly SFBaseTestAsyncFixture _fixture;
        public SFMultiStatementsIT(SFBaseTestAsyncFixture fixture, ResultFormat resultFormat) : base(fixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
        }

        [SFFact]
        public async Task TestSelectWithoutBinding()
        {
            var testDate = "2020-03-11 12:34:56 +0000";
            var testTime = "12:34:56";
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                IDbCommand cmd = conn.CreateCommand();
                var param = cmd.CreateParameter();
                param.ParameterName = "MULTI_STATEMENT_COUNT";
                param.DbType = DbType.Int16;
                param.Value = 5;
                cmd.Parameters.Add(param);
                cmd.CommandText = "select 1; select 2, 3; select 4, 5, 6;select true, false, null;" +
                                  $"select '{testDate}'::DATETIME, '{testDate}'::TIMESTAMP_TZ, '{testTime}'::TIME";
                IDataReader reader = cmd.ExecuteReader();

                Assert.True(reader.Read());
                Assert.Equal(1, reader.GetDouble(0));
                Assert.Equal(1, reader.GetFloat(0));
                Assert.Equal(1, reader.GetInt64(0));
                Assert.Equal(1, reader.GetInt32(0));
                Assert.Equal(1, reader.GetInt16(0));
                Assert.Equal(1, reader.GetByte(0));
                Assert.Equal(1L, reader.GetValue(0));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.Equal(2, reader.GetInt32(0));
                Assert.Equal(3, reader.GetInt32(1));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.Equal(4, reader.GetInt32(0));
                Assert.Equal(5, reader.GetInt32(1));
                Assert.Equal(6, reader.GetInt32(2));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.True(reader.GetBoolean(0));
                Assert.False(reader.GetBoolean(1));
                Assert.Equal(DBNull.Value, reader.GetValue(2));
                Assert.False(reader.IsDBNull(0));
                Assert.False(reader.IsDBNull(1));
                Assert.True(reader.IsDBNull(2));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.Equal(DateTime.Parse(testDate).ToUniversalTime(), reader.GetDateTime(0));
                Assert.Equal(DateTimeOffset.Parse(testDate).ToUniversalTime(), ((SnowflakeDbDataReader)reader).GetValue(1));
                Assert.Equal(TimeSpan.Parse(testTime), ((SnowflakeDbDataReader)reader).GetTimeSpan(2));
                Assert.False(reader.Read());

                Assert.False(reader.NextResult());
                Assert.False(reader.Read());

                reader.Close();
                await conn.CloseAsync();
            }
        }

        [SFFact(RetriesCount = RetriesCount.Once)]
        public async Task TestSelectAsync()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                DbCommand cmd = conn.CreateCommand();
                var param = cmd.CreateParameter();
                param.ParameterName = "MULTI_STATEMENT_COUNT";
                param.DbType = DbType.Int16;
                param.Value = 2;
                cmd.Parameters.Add(param);
                cmd.CommandText = "select 1; select 2, 3";
                DbDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                Assert.True(await reader.ReadAsync().ConfigureAwait(false));
                Assert.Equal(1, reader.GetDouble(0));
                Assert.Equal(1, reader.GetFloat(0));
                Assert.Equal(1, reader.GetInt64(0));
                Assert.Equal(1, reader.GetInt32(0));
                Assert.Equal(1, reader.GetInt16(0));
                Assert.Equal(1, reader.GetByte(0));
                Assert.Equal(1L, reader.GetValue(0));
                Assert.False(await reader.ReadAsync().ConfigureAwait(false));

                Assert.True(await reader.NextResultAsync().ConfigureAwait(false));
                Assert.True(await reader.ReadAsync().ConfigureAwait(false));
                Assert.Equal(2, reader.GetInt32(0));
                Assert.Equal(3, reader.GetInt32(1));
                Assert.False(await reader.ReadAsync().ConfigureAwait(false));

                Assert.False(await reader.NextResultAsync().ConfigureAwait(false));
                Assert.False(await reader.ReadAsync().ConfigureAwait(false));

                await reader.CloseAsync();
                await conn.CloseAsync();
            }
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public async Task TestSelectWithBinding()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                IDbCommand cmd = conn.CreateCommand();
                // Set statement count
                var stmtCountParam = cmd.CreateParameter();
                stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                stmtCountParam.DbType = DbType.Int16;
                stmtCountParam.Value = 3;
                cmd.Parameters.Add(stmtCountParam);

                // set parameter bindings
                for (int i = 1; i <= 6; i++)
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = i.ToString();
                    param.DbType = DbType.Int16;
                    param.Value = i;
                    cmd.Parameters.Add(param);
                }
                cmd.CommandText = "select ?; select ?, ?; select ?, ?, ?";
                IDataReader reader = cmd.ExecuteReader();

                Assert.True(reader.Read());
                Assert.Equal(1, reader.GetInt32(0));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.Equal(2, reader.GetInt32(0));
                Assert.Equal(3, reader.GetInt32(1));
                Assert.False(reader.Read());

                Assert.True(reader.NextResult());
                Assert.True(reader.Read());
                Assert.Equal(4, reader.GetInt32(0));
                Assert.Equal(5, reader.GetInt32(1));
                Assert.Equal(6, reader.GetInt32(2));
                Assert.False(reader.Read());

                Assert.False(reader.NextResult());

                reader.Close();
                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestMixedQueryTypeWithBinding()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"create or replace table {tableName}(cola integer, colb string);" +
                                      $"insert into {tableName} values (?, ?);" +
                                      $"insert into {tableName} values (?, ?), (?, ?);" +
                                      $"select * from {tableName};" +
                                      $"drop table if exists {tableName}";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 5;
                    cmd.Parameters.Add(stmtCountParam);

                    // set parameter bindings
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "str1";
                    cmd.Parameters.Add(p2);

                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int16;
                    p3.Value = 2;
                    cmd.Parameters.Add(p3);

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.String;
                    p4.Value = "str2";
                    cmd.Parameters.Add(p4);

                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.Int16;
                    p5.Value = 3;
                    cmd.Parameters.Add(p5);

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.String;
                    p6.Value = "str3";
                    cmd.Parameters.Add(p6);

                    DbDataReader reader = await cmd.ExecuteReaderAsync();

                    // result of create
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of insert #1
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(1, reader.RecordsAffected);

                    // result of insert #2
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(2, reader.RecordsAffected);

                    // result of select
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(-1, reader.RecordsAffected);
                    Assert.True(reader.Read());
                    Assert.Equal(1, reader.GetInt32(0));
                    Assert.Equal("str1", reader.GetString(1));
                    Assert.True(reader.Read());
                    Assert.Equal(2, reader.GetInt32(0));
                    Assert.Equal("str2", reader.GetString(1));
                    Assert.True(reader.Read());
                    Assert.Equal(3, reader.GetInt32(0));
                    Assert.Equal("str3", reader.GetString(1));
                    Assert.False(reader.Read());

                    // result of drop
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    Assert.False(reader.NextResult());
                    reader.Close();
                }

                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestMixedQueryBindingWithMultiStatementCountZero()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"use schema {_fixture.testConfig.schema};" +
                                      $"create or replace table {tableName}(cola integer, colb string);" +
                                      $"insert into {tableName} values (?, ?);" +
                                      $"insert into {tableName} values (?, ?), (?, ?);" +
                                      $"select * from {tableName};" +
                                      $"drop table if exists {tableName}";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 0;
                    cmd.Parameters.Add(stmtCountParam);

                    // set parameter bindings
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "str1";
                    cmd.Parameters.Add(p2);

                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int16;
                    p3.Value = 2;
                    cmd.Parameters.Add(p3);

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.String;
                    p4.Value = "str2";
                    cmd.Parameters.Add(p4);

                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.Int16;
                    p5.Value = 3;
                    cmd.Parameters.Add(p5);

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.String;
                    p6.Value = "str3";
                    cmd.Parameters.Add(p6);

                    DbDataReader reader = cmd.ExecuteReader();

                    //skip use statement
                    Assert.True(reader.NextResult());

                    // result of create
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of insert #1
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(1, reader.RecordsAffected);

                    // result of insert #2
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(2, reader.RecordsAffected);

                    // result of select
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(-1, reader.RecordsAffected);
                    Assert.True(reader.Read());
                    Assert.Equal(1, reader.GetInt32(0));
                    Assert.Equal("str1", reader.GetString(1));
                    Assert.True(reader.Read());
                    Assert.Equal(2, reader.GetInt32(0));
                    Assert.Equal("str2", reader.GetString(1));
                    Assert.True(reader.Read());
                    Assert.Equal(3, reader.GetInt32(0));
                    Assert.Equal("str3", reader.GetString(1));
                    Assert.False(reader.Read());

                    // result of drop
                    Assert.True(reader.NextResult());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    Assert.False(reader.NextResult());
                    reader.Close();
                }

                conn.Close();
            }
        }

        [SFFact]
        public async Task TestWithExecuteNonQuery()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"create or replace temporary table {tableName}(cola integer, colb string);" +
                                      $"insert into {tableName} values (?, ?);" +
                                      $"insert into {tableName} values (?, ?), (?, ?);" +
                                      $"select * from {tableName};" +
                                      $"drop table if exists {tableName}";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 5;
                    cmd.Parameters.Add(stmtCountParam);

                    // set parameter bindings
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "str1";
                    cmd.Parameters.Add(p2);

                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int16;
                    p3.Value = 2;
                    cmd.Parameters.Add(p3);

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.String;
                    p4.Value = "str2";
                    cmd.Parameters.Add(p4);

                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.Int16;
                    p5.Value = 3;
                    cmd.Parameters.Add(p5);

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.String;
                    p6.Value = "str3";
                    cmd.Parameters.Add(p6);

                    int count = await cmd.ExecuteNonQueryAsync();
                    Assert.Equal(3, count);
                }

                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestWithAllQueryTypes()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select 1;" +
                                      $"create or replace temporary table {tableName}(c1 varchar);" +
                                      $"explain using text select * from {tableName};" +
                                      "show parameters;" +
                                      $"insert into {tableName} values ('str1');" +
                                      $"desc table {tableName};" +
                                      $"list @%{tableName};" +
                                      $"remove @%{tableName};" +
                                      $"create or replace temporary procedure P1_{tableName}() returns varchar language javascript as $$ return ''; $$;" +
                                      $"call P1_{tableName}();" +
                                      $"use role {_fixture.testConfig.role}";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 11;
                    cmd.Parameters.Add(stmtCountParam);

                    DbDataReader reader = await cmd.ExecuteReaderAsync();

                    // result of select
                    Assert.True(reader.HasRows);
                    Assert.Equal(-1, reader.RecordsAffected);

                    // result of create
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of explain
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    // server used to return query type of explain as select
                    // but now it could be a specific type of explain
                    Assert.True((reader.RecordsAffected == 0) ||
                                  (reader.RecordsAffected == -1));

                    // result of show
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of insert
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(1, reader.RecordsAffected);

                    // result of describe
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of list
                    Assert.True(await reader.NextResultAsync());
                    Assert.False(reader.HasRows); // no files staged for table t1
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of remove
                    Assert.True(await reader.NextResultAsync());
                    Assert.False(reader.HasRows); // no files staged for table t1
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of create
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    // result of call
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    // The server behaivor is inconsistant for now, some of
                    // them returns procedure call as select while some of
                    // them use the new type.
                    Assert.True((reader.RecordsAffected == 0) ||
                                  (reader.RecordsAffected == -1));

                    // result of use
                    Assert.True(await reader.NextResultAsync());
                    Assert.True(reader.HasRows);
                    Assert.Equal(0, reader.RecordsAffected);

                    Assert.False(await reader.NextResultAsync());
                    await reader.CloseAsync();
                }

                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestWithMultipleStatementSetting()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    // MULTI_STATEMENT_COUNT=1
                    // multiple statements execution is disabled
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=1";
                    await cmd.ExecuteNonQueryAsync();

                    cmd.CommandText = "select 1; select 2; select 3";
                    try
                    {
                        await cmd.ExecuteNonQueryAsync();
                        Assert.Fail();
                    }
                    catch
                    {
                        // For now don't check the error since the error message
                        // is different between product server and test server
                    }

                    // MULTI_STATEMENT_COUNT=0
                    // multiple statements execution is enabled
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=0";
                    await cmd.ExecuteNonQueryAsync();

                    cmd.CommandText = "select 1; select 2; select 3";
                    await cmd.ExecuteNonQueryAsync();

                    // Set MULTI_STATEMENT_COUNT per query (not match)
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 4;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    try
                    {
                        await cmd.ExecuteNonQueryAsync();
                        Assert.Fail();
                    }
                    catch
                    {
                        // For now don't check the error since the error message
                        // is different between product server and test server
                    }

                    // Set MULTI_STATEMENT_COUNT per query (match)
                    cmd.Parameters.Clear();
                    stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 3;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    await cmd.ExecuteNonQueryAsync();

                    // No matter how session paramter is set
                    // parameter per query always works
                    // MULTI_STATEMENT_COUNT=0
                    // multiple statements execution is enabled
                    cmd.Parameters.Clear();
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=1";
                    await cmd.ExecuteNonQueryAsync();

                    stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 3;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    await cmd.ExecuteNonQueryAsync();
                }

                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestResultSetReturnedForAllQueryTypes()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                SessionParameterAlterer.SetResultFormat(conn, _resultFormat);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "set query_tag = (select 'dummy_tag');" +
                                      "alter session set query_tag='dummy_tag';" +
                                      "select 1;" +
                                      $"create or replace temporary table {tableName}(c1 varchar);" +
                                      $"explain using text select * from {tableName};" +
                                      "show parameters;" +
                                      $"insert into {tableName} values ('str1');" +
                                      $"update {tableName} set c1 = 'str2';" +
                                      $"select * from {tableName};" +
                                      $"desc table {tableName};" +
                                      $"copy into @%{tableName} from {tableName};" +
                                      $"list @%{tableName};" +
                                      $"remove @%{tableName};" +
                                      $"create or replace temporary procedure P1_{tableName}() returns varchar language javascript as $$ return ''; $$;" +
                                      $"call P1_{tableName}();" +
                                      $"use role {_fixture.testConfig.role}";

                    var stmtCount = 16;

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = stmtCount;
                    cmd.Parameters.Add(stmtCountParam);

                    DbDataReader reader = await cmd.ExecuteReaderAsync();

                    // at least one row in the first result set
                    Assert.True(reader.HasRows);
                    Assert.True(reader.Read());

                    for (int i = 1; i < stmtCount; i++)
                    {
                        Assert.True(reader.NextResult());

                        // at least one row in subsequent result sets
                        Assert.True(reader.HasRows);
                        Assert.True(reader.Read());
                    }
                    Assert.False(reader.NextResult());
                    reader.Close();
                }

                await conn.CloseAsync();
            }
        }
    }
}
