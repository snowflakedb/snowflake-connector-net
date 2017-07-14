using System.Data;

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
    }
}
