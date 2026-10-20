using System;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    internal class ProgrammaticAccessTokenAuthenticator : BaseAuthenticator, IAuthenticator
    {
        private const string AUTH_NAME = "programmatic_access_token";

        public ProgrammaticAccessTokenAuthenticator(SFSession session) : base(session)
        {
        }

        public static bool IsProgrammaticAccessTokenAuthenticator(string authenticator) =>
            AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            await LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Authenticate()
        {
            Login();
        }

        protected override string AuthName => AUTH_NAME;

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.Token = Session.properties[SFSessionProperty.TOKEN];
            SetSecondaryAuthenticationData(ref data);
        }
    }
}
