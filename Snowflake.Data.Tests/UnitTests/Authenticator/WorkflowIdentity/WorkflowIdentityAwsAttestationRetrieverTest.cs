using Xunit;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity
{
    public class WorkflowIdentityAwsAttestationRetrieverTest
    {
        [SFTheory]
        [InlineData("cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn")]
        [InlineData("us-east-1", "sts.us-east-1.amazonaws.com")]
        public void TestGetStsHost(string region, string expectedHost)
        {
            // act
            var host = WorkflowIdentityAwsAttestationRetriever.GetStsHostName(region);

            // assert
            Assert.Equal(expectedHost, host);
        }
    }
}
