/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security;
using System.Web;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;

namespace Snowflake.Data.Core
{
    class SFSession
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSession>();
        
        private const string SF_QUERY_SESSION_DELETE = "delete";

        private const string SF_QUERY_REQUEST_ID = "requestId";

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

        internal int connectionTimeout
        {
            get
            {
                return Int32.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);
            }
        }

        internal readonly Dictionary<SFSessionParameter, string> ParameterMap;

        /// <summary>
        ///     Constructor 
        /// </summary>
        /// <param name="connectionString">A string in the form of "key1=value1;key2=value2"</param>
        internal SFSession(String connectionString, SecureString password) : 
            this(connectionString, password, RestRequesterImpl.Instance)
        {
        }

        internal SFSession(String connectionString, SecureString password, IRestRequester restRequester)
        {
            this.restRequester = restRequester;
            properties = SFSessionProperties.parseConnectionString(connectionString, password);

            ParameterMap = new Dictionary<SFSessionParameter, string>();
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
                authenticator = new BasicAuthenticator(this);
            }

            authenticator.Authenticate();
        }

        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Session");

            if (authenticator == null)
            {
                authenticator = new BasicAuthenticator(this);
            }

            await authenticator.AuthenticateAsync(cancellationToken);
        }

        internal void close()
        {
            var queryParams = new Dictionary<string, string>();
            queryParams[SF_QUERY_SESSION_DELETE] = "true";
            queryParams[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            
            SFRestRequest closeSessionRequest = new SFRestRequest
            {
                uri = BuildUri(RestPath.SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken)
            };
          
            var response = restRequester.Post<NullDataResponse>(closeSessionRequest);
            if (!response.success)
            {
                logger.Warn($"Failed to delete session, error ignored. Code: {response.code} Message: {response.message}");
            }
        }

        internal void renewSession()
        {
            RenewSessionRequest postBody = new RenewSessionRequest()
            {
                oldSessionToken = this.sessionToken,
                requestType = "RENEW"
            };

            SFRestRequest renewSessionRequest = new SFRestRequest
            {
                jsonBody = postBody,
                uri = BuildUri(RestPath.SF_TOKEN_REQUEST_PATH,
                    new Dictionary<string, string> {{SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString()}}),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken),
                sfRestRequestTimeout = Timeout.InfiniteTimeSpan
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

