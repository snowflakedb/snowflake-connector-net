/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    using Tools;

    class MFACacheAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AUTH_NAME = "username_password_mfa";
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<MFACacheAuthenticator>();

        internal MFACacheAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
        }

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
            if (!string.IsNullOrEmpty(session._mfaToken.ToString()))
            {
                data.Token = SecureStringHelper.Decode(session._mfaToken);
            }
            SetSecondaryAuthenticationData(ref data);
        }
    }

}
