/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Security;
using System.Web;
using Newtonsoft.Json.Linq;
using Common.Logging;
using Snowflake.Data.Client;
using System.Threading;

namespace Snowflake.Data.Core
{
    class SFSession
    {
        private static readonly ILog logger = LogManager.GetLogger<SFSession>();

        private const string SF_SESSION_PATH = "/session";

        private const string SF_LOGIN_PATH = SF_SESSION_PATH + "/v1/login-request";

        private const string SF_TOKEN_REQUEST_PATH = SF_SESSION_PATH + "/token-request";

        private const string SF_QUERY_SESSION_DELETE = "delete";

        private const string SF_QUERY_WAREHOUSE = "warehouse";

        private const string SF_QUERY_DB = "databaseName";

        private const string SF_QUERY_SCHEMA = "schemaName";

        private const string SF_QUERY_ROLE = "roleName";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_BASIC = "Basic";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        internal string sessionToken { get; set; }

        private String masterToken;

        private IRestRequest restRequest;

        internal SFSessionProperties properties { get; }

        internal string database { get; set; }

        internal string schema { get; set; }

        internal string serverVersion { get; set; }

        internal int connectionTimeout
        {
            get
            {
                return Int32.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);
            }
        }

        internal Dictionary<string, string> parameterMap { get; set; }

        /// <summary>
        ///     Constructor 
        /// </summary>
        /// <param name="connectionString">A string in the form of "key1=value1;key2=value2"</param>
        internal SFSession(String connectionString, SecureString password)
        {
            restRequest = RestRequestImpl.Instance;
            properties = SFSessionProperties.parseConnectionString(connectionString);
            if (password != null)
            {
                properties[SFSessionProperty.PASSWORD] = new NetworkCredential(string.Empty, password).Password;
            }

            parameterMap = new Dictionary<string, string>();
        }

        internal void open()
        {
            logger.Debug("Open Session");

            // build uri
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_LOGIN_PATH;
            var queryString = HttpUtility.ParseQueryString(string.Empty);

            string value;
            queryString[SF_QUERY_WAREHOUSE] = properties.TryGetValue(SFSessionProperty.WAREHOUSE, out value) ? value : "";
            queryString[SF_QUERY_DB] = properties.TryGetValue(SFSessionProperty.DB, out value) ? value : "";
            queryString[SF_QUERY_SCHEMA] = properties.TryGetValue(SFSessionProperty.SCHEMA, out value) ? value : "";
            queryString[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            uriBuilder.Query = queryString.ToString();

            // build post body
            AuthnRequestClientEnv clientEnv = new AuthnRequestClientEnv()
            {
                application = System.Diagnostics.Process.GetCurrentProcess().ProcessName,
                osVersion = System.Environment.OSVersion.VersionString
            };

            AuthnRequestData data = new AuthnRequestData()
            {
                loginName = properties[SFSessionProperty.USER],
                password = properties[SFSessionProperty.PASSWORD],
                clientAppId = ".NET",
                clientAppVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString(),
                accountName = properties[SFSessionProperty.ACCOUNT],
                clientEnv = clientEnv
            };

            // build request
            int connectionTimeoutSec = Int32.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);
            SFRestRequest loginRequest = new SFRestRequest();
            loginRequest.jsonBody = new AuthnRequest() { data = data };
            loginRequest.uri = uriBuilder.Uri;
            loginRequest.authorizationToken = SF_AUTHORIZATION_BASIC;
            // total login timeout  
            if (connectionTimeoutSec <= 0) loginRequest.sfRestRequestTimeout = Timeout.InfiniteTimeSpan;
            else loginRequest.sfRestRequestTimeout = TimeSpan.FromSeconds(connectionTimeoutSec);

            if (logger.IsTraceEnabled)
            {
                logger.TraceFormat("Login Request Data: {0}", loginRequest.ToString());
            }

            JObject response = restRequest.post(loginRequest);
            parseLoginResponse(response);
        }

        internal void close()
        {
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_SESSION_PATH;

            var queryString = HttpUtility.ParseQueryString(string.Empty);
            queryString[SF_QUERY_SESSION_DELETE] = "true";
            queryString[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            uriBuilder.Query = queryString.ToString();

            SFRestRequest closeSessionRequest = new SFRestRequest();
            closeSessionRequest.jsonBody = null;
            closeSessionRequest.uri = uriBuilder.Uri;
            closeSessionRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken);

            JObject response = restRequest.post(closeSessionRequest);
            NullDataResponse deleteSessionResponse = response.ToObject<NullDataResponse>();
            if (!deleteSessionResponse.success)
            {
                logger.WarnFormat("Failed to delete session, error ignored. Code: {0} Message: {1}", 
                    deleteSessionResponse.code, deleteSessionResponse.message);
            }
        }

        internal void renewSession()
        {
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_TOKEN_REQUEST_PATH;

            var queryString = HttpUtility.ParseQueryString(string.Empty);
            queryString[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            uriBuilder.Query = queryString.ToString();

            RenewSessionRequest postBody = new RenewSessionRequest()
            {
                oldSessionToken = this.sessionToken,
                requestType = "RENEW"
            };

            SFRestRequest renewSessionRequest = new SFRestRequest();
            renewSessionRequest.jsonBody = postBody;
            renewSessionRequest.uri = uriBuilder.Uri;
            renewSessionRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken);
            renewSessionRequest.sfRestRequestTimeout = Timeout.InfiniteTimeSpan;

            JObject response = restRequest.post(renewSessionRequest);
            RenewSessionResponse sessionRenewResponse = response.ToObject<RenewSessionResponse>();
            if (!sessionRenewResponse.success)
            {
                SnowflakeDbException e = new SnowflakeDbException("", 
                    sessionRenewResponse.code, sessionRenewResponse.message, "");
                logger.Error("Renew session failed", e);
                throw e;
            } 
            else 
            {
                sessionToken = sessionRenewResponse.data.sessionToken;
            }
        }

        private void parseLoginResponse(JObject response)
        {
            AuthnResponse authnResponse = response.ToObject<AuthnResponse>();
            
            if (authnResponse.success)
            {
                sessionToken = authnResponse.data.token;
                masterToken = authnResponse.data.masterToken;
                database = authnResponse.data.authResponseSessionInfo.databaseName;
                schema = authnResponse.data.authResponseSessionInfo.schemaName;
                serverVersion = authnResponse.data.serverVersion;

                updateParameterMap(parameterMap, authnResponse.data.nameValueParameter);
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException("", authnResponse.code, authnResponse.message, "");
                logger.Error("Authentication failed", e);
                throw e;
            } 
        }

        internal static void updateParameterMap(Dictionary<string, string> parameters, List<NameValueParameter> parameterList)
        {
            logger.Debug("Update parameter map");
            foreach (NameValueParameter parameter in parameterList)
            {
                parameters[parameter.name] = parameter.value;
            }
        }
    }
}
