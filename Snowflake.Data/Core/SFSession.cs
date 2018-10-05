/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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

namespace Snowflake.Data.Core
{
    class SFSession
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSession>();

        private static readonly Tuple<AuthnRequestClientEnv, string> _EnvironmentData;

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

        static SFSession()
        {
            AuthnRequestClientEnv clientEnv = new AuthnRequestClientEnv()
            {
                application = System.Diagnostics.Process.GetCurrentProcess().ProcessName,
                osVersion = System.Environment.OSVersion.VersionString,
#if NET46
                netRuntime = "CLR:" + Environment.Version.ToString()
#else
                netRuntime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription
#endif
            };

            var clientVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();

            _EnvironmentData = Tuple.Create(clientEnv, clientVersion);
        }

        /// <summary>
        ///     Constructor 
        /// </summary>
        /// <param name="connectionString">A string in the form of "key1=value1;key2=value2"</param>
        internal SFSession(String connectionString, SecureString password) : 
            this(connectionString, password, RestRequestImpl.Instance)
        {
        }

        internal SFSession(String connectionString, SecureString password, IRestRequest restRequest)
        {
            this.restRequest = restRequest;
            properties = SFSessionProperties.parseConnectionString(connectionString, password);

            parameterMap = new Dictionary<string, string>();
        }
        
        private SFRestRequest BuildLoginRequest()
        {
            // build uri
            var queryParams = new Dictionary<string,string>();
            string warehouseValue;
            string dbValue;
            string schemaValue;
            string roleName;
            queryParams[SF_QUERY_WAREHOUSE] = properties.TryGetValue(SFSessionProperty.WAREHOUSE, out warehouseValue) ? warehouseValue : "";
            queryParams[SF_QUERY_DB] = properties.TryGetValue(SFSessionProperty.DB, out dbValue) ? dbValue : "";
            queryParams[SF_QUERY_SCHEMA] = properties.TryGetValue(SFSessionProperty.SCHEMA, out schemaValue) ? schemaValue : "";
            queryParams[SF_QUERY_ROLE] = properties.TryGetValue(SFSessionProperty.ROLE, out roleName) ? roleName : "";
            queryParams[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            
            var loginUri = BuildUri(SF_LOGIN_PATH, queryParams);

     
            AuthnRequestData data = new AuthnRequestData()
            {
                loginName = properties[SFSessionProperty.USER],
                password = properties[SFSessionProperty.PASSWORD],
                clientAppId = ".NET",
                clientAppVersion = _EnvironmentData.Item2,
                accountName = properties[SFSessionProperty.ACCOUNT],
                clientEnv = _EnvironmentData.Item1
            };

            int connectionTimeoutSec = int.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return new SFRestRequest()
            {
                jsonBody = new AuthnRequest() { data = data },
                uri = loginUri,
                authorizationToken = SF_AUTHORIZATION_BASIC,
                sfRestRequestTimeout = connectionTimeoutSec > 0 ? TimeSpan.FromSeconds(connectionTimeoutSec) : Timeout.InfiniteTimeSpan
            };
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

            var loginRequest = BuildLoginRequest();

            if (logger.IsDebugEnabled())
            {
                logger.Debug($"Login Request Data: {loginRequest.ToString()}");
            }

            var response = restRequest.Post<AuthnResponse>(loginRequest);

            ProcessLoginResponse(response);
        }

        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Session");

            var loginRequest = BuildLoginRequest();

            if (logger.IsDebugEnabled())
            {
                logger.Debug($"Login Request Data: {loginRequest.ToString()}");
            }

            var response = await restRequest.PostAsync<AuthnResponse>(loginRequest, cancellationToken);

            ProcessLoginResponse(response);
        }

        internal void close()
        {
            var queryParams = new Dictionary<string, string>();
            queryParams[SF_QUERY_SESSION_DELETE] = "true";
            queryParams[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            
            SFRestRequest closeSessionRequest = new SFRestRequest
            {
                uri = BuildUri(SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken)
            };
          
            var response = restRequest.Post<NullDataResponse>(closeSessionRequest);
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
                uri = BuildUri(SF_TOKEN_REQUEST_PATH,
                    new Dictionary<string, string> {{SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString()}}),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken),
                sfRestRequestTimeout = Timeout.InfiniteTimeSpan
            };

            logger.Info("Renew the session.");
            var response = restRequest.Post<RenewSessionResponse>(renewSessionRequest);
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

        private void ProcessLoginResponse(AuthnResponse authnResponse)
        {
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

