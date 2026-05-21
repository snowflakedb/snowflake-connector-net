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
}
