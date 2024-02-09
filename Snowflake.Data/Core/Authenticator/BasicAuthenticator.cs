﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.Authenticator
{
    class BasicAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "snowflake";
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<BasicAuthenticator>();

        internal BasicAuthenticator(SFSession session) : base(session, AuthName)
        {
        }

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            await LoginAsync(cancellationToken);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        void IAuthenticator.Authenticate()
        {
             Login();
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Only need to add the password to Data for basic authentication
            data.password = Session.properties[SFSessionProperty.PASSWORD];
        }
    }

}
