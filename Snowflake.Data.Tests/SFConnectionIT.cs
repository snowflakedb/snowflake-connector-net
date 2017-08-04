using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using Snowflake.Data.Core;

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        [Test]
        public void testLoginTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                connectionString = "host=invalidaccount.snowflakecomputing.com;connection_timeout=30;"
                    + "account=invalidaccount;user=snowman;password=test;";

                conn.ConnectionString = connectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                }
                catch(SFException e)
                {
                    Assert.AreEqual(270007, e.Data["ErrorCode"]);
                }
            }

        }

        [Test]
        public void testSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();
                Assert.AreEqual("TESTDB_DOTNET", conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);

                conn.Close();
            }

        }
    }
}
