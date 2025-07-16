using Amazon.Runtime;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class AwsConfiguration
    {
        public string Region { get; set; }
        public string Service { get; set; }
        public ImmutableCredentials Credentials { get; set; }
    }
}
