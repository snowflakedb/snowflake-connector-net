using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class SFDbCommandITSlow : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFDbCommandITSlow(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
    }

    [SFFact]
    public async Task TestLongRunningQuery()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

            await conn.OpenAsync(CancellationToken.None);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 60)) v order by 1";
            IDataReader reader = cmd.ExecuteReader();
            // only one result is returned
            Assert.True(reader.Read());

            await conn.CloseAsync(CancellationToken.None);
        }

    }
}

public sealed class SFDbCommandITSlowB : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFDbCommandITSlowB(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
    }


    [Fact(Skip = "This test case takes too much time so run it manually")]
    public async Task TestRowsAffectedOverflowInt()
    {
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString + "poolingEnabled=false"))
        {
            await conn.OpenAsync(CancellationToken.None);

            _fixture.CreateOrReplaceTable(conn, tableName, new[] { "c1 NUMBER" });

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"INSERT INTO {tableName} SELECT SEQ4() FROM TABLE(GENERATOR(ROWCOUNT=>2147484000))";
                int affected = command.ExecuteNonQuery();

                Assert.Equal(-1, affected);
            }

            await conn.CloseAsync(CancellationToken.None);
        }
    }

}

public sealed class SFDbCommandITSlowC : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFDbCommandITSlowC(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
    }

    [SFFact]
    public async Task TestExecuteWithMaxRetryReached()
    {
        var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester(false);

        using (DbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
        {
            string maxRetryConnStr = _fixture.ConnectionString + "maxHttpRetries=8;poolingEnabled=false";

            conn.ConnectionString = maxRetryConnStr;
            await conn.OpenAsync(CancellationToken.None);

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select 1;";
                    command.ExecuteScalar();
                }

                Assert.Fail();
            }
            catch (AggregateException e)
            {
                Assert.IsAssignableFrom<TaskCanceledException>(e.InnerException);
            }
            catch (Exception e)
            {
                Assert.Fail($"Expected timeout related exception, got: {e}");
            }

            stopwatch.Stop();

            var totalDelaySeconds = 1 + 2 + 4 + 8 + 16 + 16 + 16 + 16;
            // retry 8 times with backoff 1, 2, 4, 8, 16, 16, 16, 16 seconds
            // but should not delay more than another 16 seconds
            Assert.InRange(stopwatch.ElapsedMilliseconds, totalDelaySeconds * 1000, (totalDelaySeconds + 20) * 1000);
        }
    }
}

public sealed class SFDbCommandITSlowD : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFDbCommandITSlowD(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
    }

    [SFFact]
    public async Task TestExecuteAsyncWithMaxRetryReached()
    {
        var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester(false);

        using (var conn = new MockSnowflakeDbConnection(mockRestRequester))
        {
            string maxRetryConnStr = _fixture.ConnectionString + "maxHttpRetries=8;poolingEnabled=false";

            conn.ConnectionString = maxRetryConnStr;
            await conn.OpenAsync(CancellationToken.None);

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select 1;";
                    command.ExecuteScalar();
                }
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsType<TaskCanceledException>(e.InnerException);
            }
            stopwatch.Stop();

            var totalDelaySeconds = 1 + 2 + 4 + 8 + 16 + 16 + 16 + 16;
            const int MillisecondsDifferenceToAccept = 5;
            // retry 8 times with backoff 1, 2, 4, 8, 16, 16, 16, 16 seconds
            // but should not delay more than another 16 seconds
            Assert.True(stopwatch.ElapsedMilliseconds < (totalDelaySeconds + 20) * 1000);
            Assert.True(stopwatch.ElapsedMilliseconds + MillisecondsDifferenceToAccept >= totalDelaySeconds * 1000);
        }
    }
}
