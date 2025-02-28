using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthClientCredentialsAuthenticator: OAuthFlowAuthenticator, IAuthenticator
    {
        public const string AuthName = "oauth_client_credentials";

        public OAuthClientCredentialsAuthenticator(SFSession session) : base(session, AuthName)
        {
        }

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            throw new System.NotImplementedException();
        }

        public Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public void Authenticate()
        {
            throw new System.NotImplementedException();
        }
    }
}
