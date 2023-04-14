/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
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
using System.Net.Http;
using System.Text.RegularExpressions;

namespace Snowflake.Data.Core
{
    public class SFSession
    {
        public const int SF_SESSION_EXPIRED_CODE = 390112;

        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSession>();

        private static readonly Regex APPLICATION_REGEX = new Regex(@"^[A-Za-z]([A-Za-z0-9.\-_]){1,50}$");

        private const string SF_AUTHORIZATION_BASIC = "Basic";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        internal string sessionId;

        internal string sessionToken;

        internal string masterToken;

        internal IRestRequester restRequester { get; private set; }

        private IAuthenticator authenticator;

        internal SFSessionProperties properties;

        internal string database;

        internal string schema;

        internal string serverVersion;

        internal TimeSpan connectionTimeout;

        internal bool InsecureMode;

        internal bool isHeartBeatEnabled;

        private HttpClient _HttpClient;

        private string arrayBindStage = null;
        private int arrayBindStageThreshold = 0;
        internal int masterValidityInSeconds = 0;

        internal void ProcessLoginResponse(LoginResponse authnResponse)
        {
            if (authnResponse.success)
            {
                sessionId = authnResponse.data.sessionId;
                sessionToken = authnResponse.data.token;
                masterToken = authnResponse.data.masterToken;
                database = authnResponse.data.authResponseSessionInfo.databaseName;
                schema = authnResponse.data.authResponseSessionInfo.schemaName;
                serverVersion = authnResponse.data.serverVersion;
                masterValidityInSeconds = authnResponse.data.masterValidityInSeconds;
                UpdateSessionParameterMap(authnResponse.data.nameValueParameter);
                logger.Debug($"Session opened: {sessionId}");
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException
                    (SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    authnResponse.code,
                    authnResponse.message,
                    "");

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
        internal SFSession(String connectionString, SecureString password)
        {
            properties = SFSessionProperties.parseConnectionString(connectionString, password);

            // If there is an "application" setting, verify that it matches the expect pattern
            properties.TryGetValue(SFSessionProperty.APPLICATION, out string applicationNameSetting);
            if (!String.IsNullOrEmpty(applicationNameSetting) && !APPLICATION_REGEX.IsMatch(applicationNameSetting))
            {
                throw new SnowflakeDbException(
                    SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    SFError.INVALID_CONNECTION_PARAMETER_VALUE,
                    applicationNameSetting,
                    SFSessionProperty.APPLICATION.ToString()
                    );
            }

            ParameterMap = new Dictionary<SFSessionParameter, object>();
            int recommendedMinTimeoutSec = BaseRestRequest.DEFAULT_REST_RETRY_SECONDS_TIMEOUT;
            int timeoutInSec = recommendedMinTimeoutSec;
            try
            {
                ParameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS] =
                    Boolean.Parse(properties[SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS]);
                ParameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE] =
                    Boolean.Parse(properties[SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE]);
                timeoutInSec = int.Parse(properties[SFSessionProperty.CONNECTION_TIMEOUT]);
                InsecureMode = Boolean.Parse(properties[SFSessionProperty.INSECUREMODE]);
                bool disableRetry = Boolean.Parse(properties[SFSessionProperty.DISABLERETRY]);
                bool forceRetryOn404 = Boolean.Parse(properties[SFSessionProperty.FORCERETRYON404]);

                string proxyHost = null;
                string proxyPort = null;
                string noProxyHosts = null;
                string proxyPwd = null;
                string proxyUser = null;
                if (Boolean.Parse(properties[SFSessionProperty.USEPROXY]))
                {
                    // Let's try to get the associated RestRequester
                    properties.TryGetValue(SFSessionProperty.PROXYHOST, out proxyHost);
                    properties.TryGetValue(SFSessionProperty.PROXYPORT, out proxyPort);
                    properties.TryGetValue(SFSessionProperty.NONPROXYHOSTS, out noProxyHosts);
                    properties.TryGetValue(SFSessionProperty.PROXYPASSWORD, out proxyPwd);
                    properties.TryGetValue(SFSessionProperty.PROXYUSER, out proxyUser);

                    if (!String.IsNullOrEmpty(noProxyHosts))
                    {
                        // The list is url-encoded
                        // Host names are separated with a URL-escaped pipe symbol (%7C). 
                        noProxyHosts = HttpUtility.UrlDecode(noProxyHosts);
                    }
                }

                // HttpClient config based on the setting in the connection string
                HttpClientConfig httpClientConfig =
                    new HttpClientConfig(
                        !InsecureMode,
                        proxyHost,
                        proxyPort,
                        proxyUser,
                        proxyPwd,
                        noProxyHosts,
                        disableRetry,
                        forceRetryOn404);

                // Get the http client for the config
                _HttpClient = HttpUtil.Instance.GetHttpClient(httpClientConfig);
                restRequester = new RestRequester(_HttpClient);
            }
            catch (Exception e)
            {
                logger.Error("Unable to connect", e);
                throw new SnowflakeDbException(e,
                            SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                            SFError.INVALID_CONNECTION_STRING,
                            "Unable to connect");
            }

            if (timeoutInSec < recommendedMinTimeoutSec)
            {
                logger.Warn($"Connection timeout provided is less than recommended minimum value of" +
                    $" {recommendedMinTimeoutSec}");
            }
            if (timeoutInSec < 0)
            {
                logger.Warn($"Connection timeout provided is negative. Timeout will be infinite.");
            }

            connectionTimeout = timeoutInSec > 0 ? TimeSpan.FromSeconds(timeoutInSec) : Timeout.InfiniteTimeSpan;
        }

        internal SFSession(String connectionString, SecureString password, IMockRestRequester restRequester) : this(connectionString, password)
        {
            // Inject the HttpClient to use with the Mock requester
            restRequester.setHttpClient(_HttpClient);
            // Override the Rest requester with the mock for testing
            this.restRequester = restRequester;
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
            // Nothing to do if the session is not open
            if (null == sessionToken) return;

            stopHeartBeatForThisSession();

            // Send a close session request
            var queryParams = new Dictionary<string, string>();
            queryParams[RestParams.SF_QUERY_SESSION_DELETE] = "true";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            queryParams[RestParams.SF_QUERY_REQUEST_GUID] = Guid.NewGuid().ToString();

            SFRestRequest closeSessionRequest = new SFRestRequest
            {
                Url = BuildUri(RestPath.SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken)
            };

            logger.Debug($"Send closeSessionRequest");
            var response = restRequester.Post<CloseResponse>(closeSessionRequest);
            if (!response.success)
            {
                logger.Debug($"Failed to delete session: {sessionId}, error ignored. Code: {response.code} Message: {response.message}");
            }

            logger.Debug($"Session closed: {sessionId}");
            // Just in case the session won't be closed twice
            sessionToken = null;
        }

        internal async Task CloseAsync(CancellationToken cancellationToken)
        {
            // Nothing to do if the session is not open
            if (null == sessionToken) return;

            stopHeartBeatForThisSession();

            // Send a close session request
            var queryParams = new Dictionary<string, string>();
            queryParams[RestParams.SF_QUERY_SESSION_DELETE] = "true";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            queryParams[RestParams.SF_QUERY_REQUEST_GUID] = Guid.NewGuid().ToString();

            SFRestRequest closeSessionRequest = new SFRestRequest()
            {
                Url = BuildUri(RestPath.SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken)
            };

            logger.Debug($"Send async closeSessionRequest");
            var response = await restRequester.PostAsync<CloseResponse>(closeSessionRequest, cancellationToken).ConfigureAwait(false);
            if (!response.success)
            {
                logger.Debug($"Failed to delete session {sessionId}, error ignored. Code: {response.code} Message: {response.message}");
            }

            logger.Debug($"Session closed: {sessionId}");
            // Just in case the session won't be closed twice
            sessionToken = null;
        }

        internal void renewSession()
        {
            logger.Info("Renew the session.");
            var response = restRequester.Post<RenewSessionResponse>(getRenewSessionRequest());
            if (!response.success)
            {
                SnowflakeDbException e = new SnowflakeDbException("",
                    response.code, response.message, sessionId);
                logger.Error($"Renew session (ID: {sessionId}) failed", e);
                throw e;
            }
            else
            {
                sessionToken = response.data.sessionToken;
                masterToken = response.data.masterToken;
            }
        }

        internal async Task renewSessionAsync(CancellationToken cancellationToken)
        {
            logger.Info("Renew the session.");
            var response =
                    await restRequester.PostAsync<RenewSessionResponse>(
                        getRenewSessionRequest(),
                        cancellationToken
                    ).ConfigureAwait(false);
            if (!response.success)
            {
                SnowflakeDbException e = new SnowflakeDbException("",
                    response.code, response.message, sessionId);
                logger.Error($"Renew session (ID: {sessionId}) failed", e);
                throw e;
            }
            else
            {
                sessionToken = response.data.sessionToken;
                masterToken = response.data.masterToken;
            }
        }

        internal SFRestRequest getRenewSessionRequest()
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

            return new SFRestRequest
            {
                jsonBody = postBody,
                Url = BuildUri(RestPath.SF_TOKEN_REQUEST_PATH, parameters),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken),
                RestTimeout = Timeout.InfiniteTimeSpan
            };
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
            if (ParameterMap.ContainsKey(SFSessionParameter.CLIENT_STAGE_ARRAY_BINDING_THRESHOLD))
            {
                string val = ParameterMap[SFSessionParameter.CLIENT_STAGE_ARRAY_BINDING_THRESHOLD].ToString();
                this.arrayBindStageThreshold = Int32.Parse(val);
            }
            if (ParameterMap.ContainsKey(SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE))
            {
                bool keepAlive = Boolean.Parse(ParameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE].ToString());
                if(keepAlive)
                {
                    startHeartBeatForThisSession();
                }
                else
                {
                    stopHeartBeatForThisSession();
                }
            }
        }

        internal void startHeartBeatForThisSession()
        {
            if (!this.isHeartBeatEnabled)
            {
                HeartBeatBackground heartBeatBg = HeartBeatBackground.Instance;
                if (this.masterValidityInSeconds == 0)
                {
                    //In case server doesnot provide the default timeout
                    var DEFAULT_TIMEOUT_IN_SECOND = 14400;
                    this.masterValidityInSeconds = DEFAULT_TIMEOUT_IN_SECOND;
                }
                heartBeatBg.addConnection(this, this.masterValidityInSeconds);
                this.isHeartBeatEnabled = true;
            }
        }
        internal void stopHeartBeatForThisSession()
        {
            if (this.isHeartBeatEnabled)
            {
                HeartBeatBackground heartBeatBg = HeartBeatBackground.Instance;
                heartBeatBg.removeConnection(this);
                this.isHeartBeatEnabled = false;
            }

        }

        public string GetArrayBindStage()
        {
            return arrayBindStage;
        }

        public void SetArrayBindStage(string arrayBindStage)
        {
            this.arrayBindStage = string.Format("{0}.{1}.{2}", this.database, this.schema, arrayBindStage);
        }

        public int GetArrayBindStageThreshold()
        {
            return this.arrayBindStageThreshold;
        }

        public void SetArrayBindStageThreshold(int arrayBindStageThreshold)
        {
            this.arrayBindStageThreshold = arrayBindStageThreshold;
        }

        internal void heartbeat()
        {
            logger.Debug("heartbeat");

            bool retry = false;
            if (sessionToken != null)
            {
                do
                {
                    var parameters = new Dictionary<string, string>
                        {
                            { RestParams.SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString() },
                            { RestParams.SF_QUERY_REQUEST_GUID, Guid.NewGuid().ToString() },
                        };

                    SFRestRequest heartBeatSessionRequest = new SFRestRequest
                    {
                        Url = BuildUri(RestPath.SF_SESSION_HEARTBEAT_PATH, parameters),
                        authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken),
                        RestTimeout = Timeout.InfiniteTimeSpan
                    };
                    var response = restRequester.Post<NullDataResponse>(heartBeatSessionRequest);

                    logger.Debug("heartbeat response=" + response);
                    if (response.success)
                    {
                        logger.Debug("SFSession::heartbeat success, session token did not expire.");
                    }
                    else
                    {
                        if (response.code == SF_SESSION_EXPIRED_CODE)
                        {
                            logger.Debug($"SFSession ::heartbeat Session ID: {sessionId} session token expired and retry heartbeat");
                            try
                            {
                                renewSession();
                                retry = true;
                                continue;
                            }
                            catch (Exception ex)
                            {
                                // Since we don't lock the heart beat queue when sending
                                // the heart beat, it's possible that the session get
                                // closed when sending renew request and caused exception
                                // thrown from renewSession(), simply ignore that
                                logger.Error($"renew session (ID: {sessionId}) failed.", ex);
                            }
                        }
                        else
                        {
                            logger.Error($"heartbeat failed for session ID: {sessionId}.");
                        }
                    }
                    retry = false;
                } while (retry);
            }
        }
    }
}

