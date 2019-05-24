﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    class BasicAuthenticator : IAuthenticator
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<BasicAuthenticator>();
        private SFSession session;
        private const string SF_LOGIN_PATH = RestPath.SF_SESSION_PATH + "/v1/login-request";

        internal BasicAuthenticator(SFSession session)
        {
            this.session = session;
        }

        void IAuthenticator.Authenticate()
        {
            var loginRequest = BuildLoginRequest();

            var response = session.restRequester.Post<AuthnResponse>(loginRequest);

            ProcessLoginResponse(response);
        }

        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            var loginRequest = BuildLoginRequest();

            var response = await session.restRequester.PostAsync<AuthnResponse>(loginRequest, cancellationToken);

            ProcessLoginResponse(response);
        }

        private SFRestRequest BuildLoginRequest()
        {
            // build uri
            var queryParams = new Dictionary<string, string>();
            string warehouseValue;
            string dbValue;
            string schemaValue;
            string roleName;
            queryParams[RestParams.SF_QUERY_WAREHOUSE] = session.properties.TryGetValue(SFSessionProperty.WAREHOUSE, out warehouseValue) ? warehouseValue : "";
            queryParams[RestParams.SF_QUERY_DB] = session.properties.TryGetValue(SFSessionProperty.DB, out dbValue) ? dbValue : "";
            queryParams[RestParams.SF_QUERY_SCHEMA] = session.properties.TryGetValue(SFSessionProperty.SCHEMA, out schemaValue) ? schemaValue : "";
            queryParams[RestParams.SF_QUERY_ROLE] = session.properties.TryGetValue(SFSessionProperty.ROLE, out roleName) ? roleName : "";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();

            var loginUri = session.BuildUri(SF_LOGIN_PATH, queryParams);


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

            return session.BuildTimeoutRestRequest(loginUri, new AuthnRequest() { data = data });
        }

        private void ProcessLoginResponse(AuthnResponse authnResponse)
        {
            if (authnResponse.success)
            {
                session.sessionToken = authnResponse.data.token;
                session.masterToken = authnResponse.data.masterToken;
                session.database = authnResponse.data.authResponseSessionInfo.databaseName;
                session.schema = authnResponse.data.authResponseSessionInfo.schemaName;
                session.serverVersion = authnResponse.data.serverVersion;

                session.UpdateSessionParameterMap(authnResponse.data.nameValueParameter);
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException("", authnResponse.code, authnResponse.message, "");
                logger.Error("Authentication failed", e);
                throw e;
            }
        }
    }


}
