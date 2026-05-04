using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal abstract class WorkloadIdentityAttestationRetriever
    {
        protected const string SnowflakeAudience = "snowflakecomputing.com";

        /// <summary>
        /// Creates attestation data for workload identity federation authentication.
        /// </summary>
        /// <param name="snowflakeEntraResource">The Entra resource for Azure authentication.</param>
        /// <param name="token">An optional pre-provided token (for OIDC provider).</param>
        /// <param name="impersonationPath">Optional comma-separated list of identities for transitive service account impersonation.
        /// Only supported for AWS and GCP providers. Not supported for Azure or OIDC.
        /// Each identity in the chain needs permissions to impersonate the next. The final identity obtains the Snowflake connection token.
        /// For GCP: service account emails (e.g., "sa1@project.iam.gserviceaccount.com,sa2@project.iam.gserviceaccount.com").
        /// For AWS: IAM role ARNs (e.g., "arn:aws:iam::123456789012:role/Role1,arn:aws:iam::123456789012:role/Role2").</param>
        /// <returns>The attestation data to use for authentication.</returns>
        public abstract WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string token, string impersonationPath = null);

        public abstract AttestationProvider GetAttestationProvider();

        protected SnowflakeDbException AttestationError(string errorMessage) =>
            new SnowflakeDbException(SFError.WIF_ATTESTATION_ERROR, GetAttestationProvider().ToString(), errorMessage);
    }
}
