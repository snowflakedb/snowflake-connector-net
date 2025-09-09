using System;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    internal class ProgrammaticAccessTokenAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "programmatic_access_token";

        public ProgrammaticAccessTokenAuthenticator(SFSession session) : base(session, AuthName)
        {
        }

        public static bool IsProgrammaticAccessTokenAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            await LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Authenticate()
        {
            Login();
        }

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.Token = session.properties[SFSessionProperty.TOKEN];
            SetSecondaryAuthenticationData(ref data);
        }
    }
}
