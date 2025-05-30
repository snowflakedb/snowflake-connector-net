using System;
using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    class BasicAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AUTH_NAME = "snowflake";
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<BasicAuthenticator>();

        internal BasicAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
        }

        public static bool IsBasicAuthenticator(string authenticator) =>
            AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            await base.LoginAsync(cancellationToken);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        void IAuthenticator.Authenticate()
        {
            base.Login();
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Only need to add the password to Data for basic authentication
            data.password = session.properties[SFSessionProperty.PASSWORD];
            SetSecondaryAuthenticationData(ref data);
        }
    }

}
