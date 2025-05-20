using System;
using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// OAuthenticator is used when using  an OAuth token for authentication.
    /// See <see cref="https://docs.snowflake.com/en/user-guide/oauth.html"/> for more information.
    /// </summary>
    class OAuthAuthenticator : BaseAuthenticator, IAuthenticator
    {
        // The authenticator setting value to use to authenticate using key pair authentication.
        public const string AUTH_NAME = "oauth";

        // The logger.
        private static readonly SFLogger logger =
            SFLoggerFactory.GetLogger<OAuthAuthenticator>();

        /// <summary>
        /// Constructor for the oauth authenticator.
        /// </summary>
        /// <param name="session">Session which created this authenticator</param>
        internal OAuthAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
            this.session = session;
        }

        public static bool IsOAuthAuthenticator(string authenticator)
            => AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        /// <see cref="IAuthenticator.Authenticate"/>
        public void Authenticate()
        {
            base.Login();
        }

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async public Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            await base.LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Add the token to the Data attribute
            data.Token = session.properties[SFSessionProperty.TOKEN];
            // Remove the login name for an OAuth session
            data.loginName = "";
            SetSecondaryAuthenticationData(ref data);
        }
    }
}
