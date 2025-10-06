using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Authenticator
{
    class MFACacheAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "username_password_mfa";
        private const int MfaLoginHttpTimeout = 60;

        internal MFACacheAuthenticator(SFSession session) : base(session, AuthName)
        {
        }

        public static bool IsMfaCacheAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

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
            data.SessionParameters[SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN] = true;
            data.HttpTimeout = TimeSpan.FromSeconds(MfaLoginHttpTimeout);
            if (!string.IsNullOrEmpty(session._mfaToken?.ToString()))
            {
                data.Token = SecureStringHelper.Decode(session._mfaToken);
            }
            SetSecondaryAuthenticationData(ref data);
        }
    }

}
