using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests;

[TestFixture]
class SFDbCommandITSlow : SFBaseTest
{
    [Test]
    public void TestLongRunningQuery()
    {
        using (IDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString + "poolingEnabled=false";

            conn.Open();

            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 60)) v order by 1";
            IDataReader reader = cmd.ExecuteReader();
            // only one result is returned
            Assert.IsTrue(reader.Read());

            conn.Close();
        }

    }

    [Test]
    [Ignore("This test case takes too much time so run it manually")]
    public void TestRowsAffectedOverflowInt()
    {
        using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false"))
        {
            conn.Open();

            CreateOrReplaceTable(conn, TableName, new[] { "c1 NUMBER" });

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"INSERT INTO {TableName} SELECT SEQ4() FROM TABLE(GENERATOR(ROWCOUNT=>2147484000))";
                int affected = command.ExecuteNonQuery();

                Assert.AreEqual(-1, affected);
            }
            conn.Close();
        }
    }

    [Test]
    public void TestExecuteAsyncWithMaxRetryReached()
    {
        var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester(false);

        using (DbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
        {
            string maxRetryConnStr = ConnectionString + "maxHttpRetries=8;poolingEnabled=false";

            conn.ConnectionString = maxRetryConnStr;
            conn.Open();

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select 1;";
                    Task<object> t = command.ExecuteScalarAsync();
                    t.Wait();
                }

                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsInstanceOf<TaskCanceledException>(e.InnerException);
            }

            stopwatch.Stop();

            var totalDelaySeconds = 1 + 2 + 4 + 8 + 16 + 16 + 16 + 16;
            // retry 8 times with backoff 1, 2, 4, 8, 16, 16, 16, 16 seconds
            // but should not delay more than another 16 seconds
            Assert.Less(stopwatch.ElapsedMilliseconds, (totalDelaySeconds + 20) * 1000);
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, totalDelaySeconds * 1000);
        }
    }
}
