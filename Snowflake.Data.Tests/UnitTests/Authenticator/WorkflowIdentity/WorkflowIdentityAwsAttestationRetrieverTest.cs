using NUnit.Framework;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity
{
    [TestFixture]
    public class WorkflowIdentityAwsAttestationRetrieverTest
    {
        [Test]
        [TestCase("cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn")]
        [TestCase("us-east-1", "sts.us-east-1.amazonaws.com")]
        public void TestGetStsHost(string region, string expectedHost)
        {
            // act
            var host = WorkflowIdentityAwsAttestationRetriever.GetStsHostName(region);

            // assert
            Assert.AreEqual(expectedHost, host);
        }
    }
}
