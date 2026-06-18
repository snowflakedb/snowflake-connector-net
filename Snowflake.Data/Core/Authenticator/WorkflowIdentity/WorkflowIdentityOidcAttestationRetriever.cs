using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityOidcAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityOidcAttestationRetriever>();

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.OIDC;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string tokenParam, string impersonationPath = null)
        {
            if (!string.IsNullOrEmpty(impersonationPath))
            {
                throw AttestationError("Impersonation is not supported for OIDC workload identity provider");
            }
            if (string.IsNullOrEmpty(tokenParam))
            {
                throw AttestationError("No token provided");
            }
            JwtSecurityToken token;
            try
            {
                token = new JwtTokenExtractor().ReadJwtToken(tokenParam, AttestationError);
            }
            catch (Exception)
            {
                var errorMessage = "Failed to parse a token for OIDC workload identity federation.";
                s_logger.Error(errorMessage);
                throw AttestationError(errorMessage);
            }
            var issuer = token.Issuer;
            var subject = token.Subject;
            return new WorkloadIdentityAttestationData
            {
                Provider = AttestationProvider.OIDC,
                Credential = tokenParam,
                UserIdentifierComponents = new Dictionary<string, string>
                {
                    { "iss", issuer },
                    { "sub", subject }
                }
            };
        }
    }
}
