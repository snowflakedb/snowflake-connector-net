using Amazon.Runtime;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class AwsSdkWrapper
    {
        public static AwsSdkWrapper Instance = new AwsSdkWrapper();

        public virtual ImmutableCredentials GetAwsCredentials() =>
#pragma warning disable CS0618 // Type or member is obsolete
            FallbackCredentialsFactory.GetCredentials()?.GetCredentials();
#pragma warning restore CS0618 // Type or member is obsolete

        public virtual string GetAwsRegion() =>
            FallbackRegionFactory.GetRegionEndpoint()?.SystemName;
    }
}
