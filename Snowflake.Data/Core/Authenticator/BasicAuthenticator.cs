/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    class BasicAuthenticator : IAuthenticator
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<BasicAuthenticator>();
        private SFSession session;

        internal BasicAuthenticator(SFSession session)
        {
            this.session = session;
        }

        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            var loginRequest = BuildLoginRequest();

            var response = await session.restRequester.PostAsync<AuthnResponse>(loginRequest, cancellationToken);

            session.ProcessLoginResponse(response);
        }

        private SFRestRequest BuildLoginRequest()
        {
            // build uri
            var loginUrl = session.BuildLoginUrl();

            AuthnRequestData data = new AuthnRequestData()
            {
                loginName = session.properties[SFSessionProperty.USER],
                password = session.properties[SFSessionProperty.PASSWORD],
                accountName = session.properties[SFSessionProperty.ACCOUNT],
                clientAppId = ".NET",
                clientAppVersion = SFEnvironment.Version,
                clientEnv = SFEnvironment.ClientEnv,
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(loginUrl, new AuthnRequest() { data = data });
        }

    }

}
