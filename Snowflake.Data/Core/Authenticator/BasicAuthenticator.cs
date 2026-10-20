using System;
using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    class BasicAuthenticator : BaseAuthenticator, IAuthenticator
    {
        private const string s_AuthName = "snowflake";

        internal BasicAuthenticator(SFSession session) : base(session)
        {
        }

        public static bool IsBasicAuthenticator(string authenticator) =>
            s_AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            await base.LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        void IAuthenticator.Authenticate()
        {
            base.Login();
        }

        protected override string AuthName => s_AuthName;

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Only need to add the password to Data for basic authentication
            data.password = Session.properties[SFSessionProperty.PASSWORD];
            SetSecondaryAuthenticationData(ref data);
        }
    }

}
