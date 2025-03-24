using System;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    internal class ProgrammaticAccessTokenAuthenticator: BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "programmatic_access_token";

        public ProgrammaticAccessTokenAuthenticator(SFSession session) : base(session, AuthName)
        {
        }

        public static bool IsProgrammaticAccessTokenAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            await LoginAsync(cancellationToken);
        }

        public void Authenticate()
        {
            Login();
        }

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.Token = GetPatToken();
            SetSecondaryAuthenticationData(ref data);
        }

        private string GetPatToken()
        {
            if (session.properties.TryGetValue(SFSessionProperty.TOKEN, out var token) && !string.IsNullOrEmpty(token))
            {
                return token;
            }
            if (session.properties.TryGetValue(SFSessionProperty.PASSWORD, out var password) && !string.IsNullOrEmpty(password))
            {
                return password;
            }
            return null;
        }
    }
}
