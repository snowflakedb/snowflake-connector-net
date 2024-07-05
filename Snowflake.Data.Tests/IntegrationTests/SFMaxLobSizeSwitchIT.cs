using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class SFMaxLobSizeSwitchIT : SFBaseTest
    {
        [Test]
        [Ignore("TODO: Enable when Max LOB size is available on the automated tests environment")]
        public void TestIncreaseMaxLobSizeParameterSwitchSelect()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                IDbCommand cmd = conn.CreateCommand();

                //setup
                cmd.CommandText = "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY='ENABLED'";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=false";
                cmd.ExecuteNonQuery();

                //initial test
                try
                {
                    cmd.CommandText = "select randstr(20000000, random()) as large_str";
                    cmd.ExecuteReader();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.That(e.Message, Does.Contain("exceeds supported length"));
                }

                //parameter switch
                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=true";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "select randstr(20000000, random()) as large_str";
                var reader = cmd.ExecuteReader();
                Assert.IsTrue(reader.Read());

                cmd.CommandText = "alter session unset ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "alter session unset ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT";
                cmd.ExecuteNonQuery();
            }
        }
    }
}
