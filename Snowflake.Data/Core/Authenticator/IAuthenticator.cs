/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal interface IAuthenticator
    {
        void Authenticate();

        Task AuthenticateAsync(CancellationToken cancellationToken);
    }

    internal enum SFAuthenticatorType
    {
        SNOWFLAKE,
        OKTA,
    }

    internal class AuthenticatorFactory
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<AuthenticatorFactory>();

        internal IAuthenticator GetAuthenticator(SFSession session)
        {
            string type = session.properties[SFSessionProperty.AUTHENTICATOR];
            switch (type)
            {
                case "snowflake":
                    return new BasicAuthenticator(session);
            }

            if (type.EndsWith("okta.com") && type.StartsWith("https://"))
            {
                return new OktaAuthenticator(session, type);
            }

            var e = new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR, new object[] { type });

            logger.Error("Unknown authenticator", e);

            throw e;
        }
    }
}