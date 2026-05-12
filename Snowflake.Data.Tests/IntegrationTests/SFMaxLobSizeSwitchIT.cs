using System.Data;
using Xunit;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class SFMaxLobSizeSwitchIT : SFBaseTest
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFMaxLobSizeSwitchIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        private const string SqlSelectLargeString = "select randstr(20000000, random()) as large_str";

        [Fact(Skip = "TODO: Enable when Max LOB size is available on the automated tests environment")]
        public void TestIncreaseMaxLobSizeParameterSwitchSelect()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString + "poolingEnabled=false"))
            {
                conn.Open();
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=false";
                cmd.ExecuteNonQuery();

                cmd.CommandText = SqlSelectLargeString;
                var thrown = Assert.Throws<SnowflakeDbException>(() => cmd.ExecuteReader());
                Assert.Contains("exceeds supported length", thrown.Message);

                cmd.CommandText = "alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=true";
                cmd.ExecuteNonQuery();
                cmd.CommandText = SqlSelectLargeString;
                var reader = cmd.ExecuteReader();
                Assert.True(reader.Read());
            }
        }
    }
}
