using Amazon.Runtime;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class AwsSdkWrapper
    {
        public static AwsSdkWrapper Instance = new AwsSdkWrapper();

        public virtual ImmutableCredentials GetAwsCredentials() =>
            FallbackCredentialsFactory.GetCredentials()?.GetCredentials();

        public virtual string GetAwsRegion() =>
            FallbackRegionFactory.GetRegionEndpoint()?.SystemName;
    }
}
