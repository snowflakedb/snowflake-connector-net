using System.Collections.Generic;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkloadIdentityAttestationData
    {
        public AttestationProvider Provider { get; set; }
        public string Credential { get; set; }
        public Dictionary<string, string> UserIdentifierComponents { get; set; }
    }
}
