﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Web;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    class SFSession
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSession>();

        private const string SF_AUTHORIZATION_BASIC = "Basic";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        internal string sessionToken;

        internal string masterToken;

        internal IRestRequester restRequester;

        private IAuthenticator authenticator;

        internal SFSessionProperties properties;

        internal string database;

        internal string schema;

        internal string serverVersion;

        internal TimeSpan connectionTimeout;

        internal void ProcessLoginResponse(LoginResponse authnResponse)
        {
            if (authnResponse.success)
            {
                sessionToken = authnResponse.data.token;
                masterToken = authnResponse.data.masterToken;
                database = authnResponse.data.authResponseSessionInfo.databaseName;
                schema = authnResponse.data.authResponseSessionInfo.schemaName;
                serverVersion = authnResponse.data.serverVersion;

                UpdateSessionParameterMap(authnResponse.data.nameValueParameter);
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException("", authnResponse.code, authnResponse.message, "");
                logger.Error("Authentication failed", e);
                throw e;
            }
        }

        internal readonly Dictionary<SFSessionParameter, Object> ParameterMap;

        internal Uri BuildLoginUrl()
        {
            var queryParams = new Dictionary<string, string>();
            string warehouseValue;
            string dbValue;
            string schemaValue;
            string roleName;
            queryParams[RestParams.SF_QUERY_WAREHOUSE] = properties.TryGetValue(SFSessionProperty.WAREHOUSE, out warehouseValue) ? warehouseValue : "";
            queryParams[RestParams.SF_QUERY_DB] = properties.TryGetValue(SFSessionProperty.DB, out dbValue) ? dbValue : "";
            queryParams[RestParams.SF_QUERY_SCHEMA] = properties.TryGetValue(SFSessionProperty.SCHEMA, out schemaValue) ? schemaValue : "";
            queryParams[RestParams.SF_QUERY_ROLE] = properties.TryGetValue(SFSessionProperty.ROLE, out roleName) ? roleName : "";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            queryParams[RestParams.SF_QUERY_REQUEST_GUID] = Guid.NewGuid().ToString();

            var loginUrl = BuildUri(RestPath.SF_LOGIN_PATH, queryParams);
            return loginUrl;
        }

        /// <summary>
        ///     Constructor 
        /// </summary>
        /// <param name="connectionString">A string in the form of "key1=value1;key2=value2"</param>
        internal SFSession(String connectionString, SecureString password) : 
            this(connectionString, password, RestRequester.Instance)
        {
        }

        internal SFSession(String connectionString, SecureString password, IRestRequester restRequester)
        {
            this.restRequester = restRequester;
            properties = SFSessionProperties.parseConnectionString(connectionString, password);

            ParameterMap = new Dictionary<SFSessionParameter, object>();
            ParameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS] = 
                Boolean.Parse(properties[SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS]);

            int timeoutInSec = int.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);
            connectionTimeout = timeoutInSec > 0 ? TimeSpan.FromSeconds(timeoutInSec) : Timeout.InfiniteTimeSpan;
        }

        internal Uri BuildUri(string path, Dictionary<string, string> queryParams = null)
        {
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = properties[SFSessionProperty.HOST];
            uriBuilder.Port = int.Parse(properties[SFSessionProperty.PORT]);
            uriBuilder.Path = path;

            if (queryParams != null && queryParams.Any())
            {
                var queryString = HttpUtility.ParseQueryString(string.Empty);
                foreach (var kvp in queryParams)
                    queryString[kvp.Key] = kvp.Value;

                uriBuilder.Query = queryString.ToString();
            }
            
            return uriBuilder.Uri;
        }

        internal void Open()
        {
            logger.Debug("Open Session");

            if (authenticator == null)
            {
                authenticator = AuthenticatorFactory.GetAuthenticator(this);
            }

            authenticator.Authenticate();
        }

        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Session");

            if (authenticator == null)
            {
                authenticator = AuthenticatorFactory.GetAuthenticator(this);
            }

            await authenticator.AuthenticateAsync(cancellationToken).ConfigureAwait(false);
        }

        internal void close()
        {
            var queryParams = new Dictionary<string, string>();
            queryParams[RestParams.SF_QUERY_SESSION_DELETE] = "true";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            queryParams[RestParams.SF_QUERY_REQUEST_GUID] = Guid.NewGuid().ToString();

            SFRestRequest closeSessionRequest = new SFRestRequest
            {
                Url = BuildUri(RestPath.SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken)
            };
          
            var response = restRequester.Post<CloseResponse>(closeSessionRequest);
            if (!response.success)
            {
                logger.Debug($"Failed to delete session, error ignored. Code: {response.code} Message: {response.message}");
            }
        }

        internal void renewSession()
        {
            RenewSessionRequest postBody = new RenewSessionRequest()
            {
                oldSessionToken = this.sessionToken,
                requestType = "RENEW"
            };

            var parameters = new Dictionary<string, string>
                {
                    { RestParams.SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString() },
                    { RestParams.SF_QUERY_REQUEST_GUID, Guid.NewGuid().ToString() },
                };

            SFRestRequest renewSessionRequest = new SFRestRequest
            {
                jsonBody = postBody,
                
                Url = BuildUri(RestPath.SF_TOKEN_REQUEST_PATH, parameters),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken),
                RestTimeout = Timeout.InfiniteTimeSpan
            };

            logger.Info("Renew the session.");
            var response = restRequester.Post<RenewSessionResponse>(renewSessionRequest);
            if (!response.success)
            {
                SnowflakeDbException e = new SnowflakeDbException("", 
                    response.code, response.message, "");
                logger.Error("Renew session failed", e);
                throw e;
            } 
            else 
            {
                sessionToken = response.data.sessionToken;
                masterToken = response.data.masterToken;
            }
        }

        internal SFRestRequest BuildTimeoutRestRequest(Uri uri, Object body)
        {
            return new SFRestRequest()
            {
                jsonBody = body,
                Url = uri,
                authorizationToken = SF_AUTHORIZATION_BASIC,
                RestTimeout = connectionTimeout,
            };
        }
        
        internal void UpdateSessionParameterMap(List<NameValueParameter> parameterList)
        {
            logger.Debug("Update parameter map");
            foreach (NameValueParameter parameter in parameterList)
            {
                if (Enum.TryParse(parameter.name, out SFSessionParameter parameterName))
                {
                    ParameterMap[parameterName] = parameter.value;
                }
            }
        }
    }
}

