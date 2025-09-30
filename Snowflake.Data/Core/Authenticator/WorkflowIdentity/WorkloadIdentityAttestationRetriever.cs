using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal abstract class WorkloadIdentityAttestationRetriever
    {
        protected const string SnowflakeAudience = "snowflakecomputing.com";

        public abstract WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string token);

        public abstract AttestationProvider GetAttestationProvider();

        protected SnowflakeDbException AttestationError(string errorMessage) =>
            new SnowflakeDbException(SFError.WIF_ATTESTATION_ERROR, GetAttestationProvider().ToString(), errorMessage);
    }
}
