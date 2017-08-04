using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;

    [TestFixture]    
    class SFDbCommandIT : SFBaseTest
    {
        [Test]
        public void testSimpleLargeResultSet()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 200000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    counter++;
                }
                conn.Close();
            }
        }

        [Test]
        public void testLongRunningQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
        public void testDataSourceError()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select * from table_not_exists";
                try
                {
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(e.ErrorCode, 2003);
                }

                conn.Close();
            }
        }

        [Test]
        public void testCancelQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                Task executionThread = Task.Run(() =>
                {
                    try
                    {
                        cmd.ExecuteScalar();
                        Assert.Fail();
                    }
                    catch(SnowflakeDbException e)
                    {
                        Assert.AreEqual(e.ErrorCode, 604);
                    }
                });

                Thread.Sleep(5000);
                cmd.Cancel();

                executionThread.Wait();

                conn.Close();
            }
        }

        [Test]
        public void testQueryTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                cmd.CommandTimeout = 10;

                try
                {
                    cmd.ExecuteScalar();
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(e.ErrorCode, 604);
                }

                conn.Close();
            }

        }
    }
}
