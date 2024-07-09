using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class SFMaxLobSizeSwitchIT : SFBaseTest
    {
        private const string SqlSelectLargeString = "select randstr(20000000, random()) as large_str";

        [Test]
        [Ignore("TODO: Enable when Max LOB size is available on the automated tests environment")]
        public void TestIncreaseMaxLobSizeParameterSwitchSelect()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false"))
            {
                conn.Open();
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=false";
                cmd.ExecuteNonQuery();

                cmd.CommandText = SqlSelectLargeString;
                var thrown = Assert.Throws<SnowflakeDbException>(() => cmd.ExecuteReader());
                Assert.That(thrown.Message, Does.Contain("exceeds supported length"));

                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=true";
                cmd.ExecuteNonQuery();
                cmd.CommandText = SqlSelectLargeString;
                var reader = cmd.ExecuteReader();
                Assert.IsTrue(reader.Read());
            }
        }
    }
}
