using System.Threading;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using Xunit;
    using System.Data;
    using System.Threading.Tasks;
    using Snowflake.Data.Client;
    using System.Data.Common;
    public class SFStatementTypeTest : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFStatementTypeTest(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact]
        public async Task TestCallStatement()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (DbCommand command = conn.CreateCommand())
                {
                    try
                    {
                        command.CommandText = "ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS=true";
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    catch (SnowflakeDbException ex)
                    {
                        // Expected error - SqlState: 22023, VendorCode: 1006
                        Assert.Equal("22023", ex.SqlState);
                    }

                    command.CommandText = "create or replace procedure\n"
                        + "TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant)\n"
                        + "returns string language javascript as $$\n"
                        + "let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]});\n"
                        + "res.next();\n"
                        + "return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2;\n"
                        + "$$;";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                    command.CommandText = "call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Double;
                    p1.Value = 1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "[2,3]";
                    command.Parameters.Add(p2);

                    DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                    string result = "1 \"[2,3]\" [2,3]";
                    while (reader.Read())
                    {
                        Assert.Equal(result, reader.GetString(0));
                    }

                    command.CommandText = "drop procedure if exists TEST_SP_CALL_STMT_ENABLED(float, variant)";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                await conn.CloseAsync().ConfigureAwait(false);
            }
        }
    }
}
