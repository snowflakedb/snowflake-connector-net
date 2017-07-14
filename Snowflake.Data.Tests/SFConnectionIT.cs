using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        [Test]
        public void testConnectionTimeout()
        {

        }

        [Test]
        public void testSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();
                Assert.AreEqual("TESTDB_DOTNET", conn.Database);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);

                conn.Close();
            }

        }
    }
}
