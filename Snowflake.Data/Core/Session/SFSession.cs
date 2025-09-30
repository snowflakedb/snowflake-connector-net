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
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core
{
    internal class SFSession
    {
        public const int SF_SESSION_EXPIRED_CODE = 390112;

        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSession>();

        private static readonly Regex APPLICATION_REGEX = new Regex(@"^[A-Za-z]([A-Za-z0-9.\-_]){1,50}$");

        private const string SF_AUTHORIZATION_BASIC = "Basic";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private const int _defaultQueryContextCacheSize = 5;

        internal string sessionId;

        internal string sessionToken;

        internal string masterToken;

        internal IRestRequester restRequester { get; private set; }

        internal IAuthenticator authenticator;

        internal SFSessionProperties properties;

        internal string database;
        internal string schema;
        internal string role;
        internal string warehouse;
        internal bool sessionPropertiesChanged = false;

        internal string serverVersion;

        private readonly ConnectionPoolConfig _poolConfig;

        internal TimeSpan connectionTimeout => _poolConfig.ConnectionTimeout;

        internal bool isHeartBeatEnabled;

        private HttpClient _HttpClient;

        private string arrayBindStage = null;
        private int arrayBindStageThreshold = 0;
        internal int masterValidityInSeconds = 0;

        private readonly EasyLoggingStarter _easyLoggingStarter = EasyLoggingStarter.Instance;

        private long _startTime = 0;
        internal string ConnectionString { get; }

        internal SessionPropertiesContext PropertiesContext { get; }

        private QueryContextCache _queryContextCache = new QueryContextCache(_defaultQueryContextCacheSize);

        private int _queryContextCacheSize = _defaultQueryContextCacheSize;

        private bool _disableQueryContextCache = false;

        internal bool _disableConsoleLogin;

        internal int _maxRetryCount;

        internal TimeSpan _maxRetryTimeout;

        private string _user;

        internal bool _disableSamlUrlCheck;

        public bool GetPooling() => _poolConfig.PoolingEnabled;

        public void SetPooling(bool isEnabled)
        {
            _poolConfig.PoolingEnabled = isEnabled;
        }

        internal String _queryTag;

        internal SecureString _mfaToken;

        internal void ProcessLoginResponse(LoginResponse authnResponse)
        {
            if (authnResponse.success)
            {
                sessionId = authnResponse.data.sessionId;
                sessionToken = authnResponse.data.token;
                masterToken = authnResponse.data.masterToken;
                database = authnResponse.data.authResponseSessionInfo.databaseName;
                schema = authnResponse.data.authResponseSessionInfo.schemaName;
                role = authnResponse.data.authResponseSessionInfo.roleName;
                warehouse = authnResponse.data.authResponseSessionInfo.warehouseName;
                serverVersion = authnResponse.data.serverVersion;
                masterValidityInSeconds = authnResponse.data.masterValidityInSeconds;
                UpdateSessionParameterMap(authnResponse.data.nameValueParameter);
                if (_disableQueryContextCache)
                {
                    logger.Debug("Query context cache disabled.");
                }

                if (bool.Parse(properties[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]) &&
                    !string.IsNullOrEmpty(_user) && !string.IsNullOrEmpty(authnResponse.data.idToken))
                {
                    var idTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(properties[SFSessionProperty.HOST], properties[SFSessionProperty.USER], TokenType.IdToken);
                    SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(idTokenKey, authnResponse.data.idToken);
                }
                if (!string.IsNullOrEmpty(authnResponse.data.mfaToken))
                {
                    _mfaToken = SecureStringHelper.Encode(authnResponse.data.mfaToken);
                    var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(properties[SFSessionProperty.HOST], properties[SFSessionProperty.USER], TokenType.MFAToken);
                    SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(key, authnResponse.data.mfaToken);
                }
                logger.Debug($"Session opened: {sessionId}");
                _startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException
                    (SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    authnResponse.code,
                    authnResponse.message,
                    "");

                logger.Error("Authentication failed", e);

                if (SFMFATokenErrors.IsInvalidMFATokenContinueError(e.ErrorCode))
                {
                    logger.Info($"Unable to use cached MFA token is expired or invalid. Fails with the {e.Message}. ", e);
                    _mfaToken = null;
                    var mfaKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(properties[SFSessionProperty.HOST], properties[SFSessionProperty.USER], TokenType.MFAToken);
                    SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(mfaKey);
                }
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
        internal SFSession(
            String connectionString,
            SessionPropertiesContext sessionContext) : this(connectionString, sessionContext, EasyLoggingStarter.Instance)
        {
        }

        internal SFSession(
            String connectionString,
            SessionPropertiesContext sessionContext,
            EasyLoggingStarter easyLoggingStarter)
        {
            _easyLoggingStarter = easyLoggingStarter;
            ConnectionString = connectionString;
            PropertiesContext = sessionContext;
            properties = SFSessionProperties.ParseConnectionString(ConnectionString, sessionContext);
            _disableQueryContextCache = bool.Parse(properties[SFSessionProperty.DISABLEQUERYCONTEXTCACHE]);
            _disableConsoleLogin = bool.Parse(properties[SFSessionProperty.DISABLE_CONSOLE_LOGIN]);
            properties.TryGetValue(SFSessionProperty.USER, out _user);
            ValidateApplicationName(properties);
            try
            {
                var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);
                var httpClientConfig = extractedProperties.BuildHttpClientConfig();
                ParameterMap = extractedProperties.ToParameterMap();
                _HttpClient = HttpUtil.Instance.GetHttpClient(httpClientConfig);
                restRequester = new RestRequester(_HttpClient);
                _poolConfig = extractedProperties.BuildConnectionPoolConfig();
                properties.TryGetValue(SFSessionProperty.CLIENT_CONFIG_FILE, out var easyLoggingConfigFile);
                _easyLoggingStarter.Init(easyLoggingConfigFile);
                properties.TryGetValue(SFSessionProperty.QUERY_TAG, out _queryTag);
                _maxRetryCount = extractedProperties.maxHttpRetries;
                _maxRetryTimeout = extractedProperties.retryTimeout;
                _disableSamlUrlCheck = extractedProperties._disableSamlUrlCheck;

                if (properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var _authenticatorType) &&
                    MFACacheAuthenticator.IsMfaCacheAuthenticator(_authenticatorType))
                {
                    var mfaKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(properties[SFSessionProperty.HOST], properties[SFSessionProperty.USER], TokenType.MFAToken);
                    _mfaToken = SecureStringHelper.Encode(SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(mfaKey));
                }
            }
            catch (SnowflakeDbException e)
            {
                logger.Error("Unable to initialize session ", e);
                throw;
            }
            catch (Exception e)
            {
                logger.Error("Unable to initialize session ", e);
                throw new SnowflakeDbException(e,
                            SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                            SFError.INVALID_CONNECTION_STRING,
                            "Unable to initialize session ");
            }
        }

        internal SFSession(String connectionString, SessionPropertiesContext sessionContext, IMockRestRequester restRequester) : this(connectionString, sessionContext, EasyLoggingStarter.Instance, restRequester)
        {
        }

        internal SFSession(String connectionString, SessionPropertiesContext sessionContext, EasyLoggingStarter easyLoggingStarter, IMockRestRequester restRequester) : this(connectionString, sessionContext, easyLoggingStarter)
        {
            // Inject the HttpClient to use with the Mock requester
            restRequester.setHttpClient(_HttpClient);
            // Override the Rest requester with the mock for testing
            this.restRequester = restRequester;
        }

        internal bool IsPoolingEnabledForConnectionCache()
        {
            var authenticator = properties[SFSessionProperty.AUTHENTICATOR];
            var forbiddenAuthenticators = new Func<string, bool>[]
            {
                OAuthAuthorizationCodeAuthenticator.IsOAuthAuthorizationCodeAuthenticator,
                OAuthClientCredentialsAuthenticator.IsOAuthClientCredentialsAuthenticator,
                ProgrammaticAccessTokenAuthenticator.IsProgrammaticAccessTokenAuthenticator
            };
            return !forbiddenAuthenticators.Any(f => f.Invoke(authenticator));
        }

        private void ValidateApplicationName(SFSessionProperties properties)
        {
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

        internal virtual void Open()
        {
            logger.Debug("Open Session");
            InitialiseAuthenticator();
            authenticator.Authenticate();
        }

        internal virtual async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Session Async");
            InitialiseAuthenticator();
            await authenticator.AuthenticateAsync(cancellationToken).ConfigureAwait(false);
        }

        internal void InitialiseAuthenticator()
        {
            if (authenticator == null)
            {
                authenticator = AuthenticatorFactory.GetAuthenticator(this);
            }
        }

        internal void close()
        {
            // Nothing to do if the session is not open
            if (!IsEstablished()) return;
            logger.Debug($"Closing session with id: {sessionId}, user: {_user}, database: {database}, schema: {schema}, role: {role}, warehouse: {warehouse}, connection start timestamp: {_startTime}");
            stopHeartBeatForThisSession();
            var closeSessionRequest = PrepareCloseSessionRequest();
            PostCloseSession(closeSessionRequest, restRequester);
            sessionToken = null;
        }

        internal void CloseNonBlocking()
        {
            // Nothing to do if the session is not open
            if (!IsEstablished()) return;
            logger.Debug($"Closing session with id: {sessionId}, user: {_user}, database: {database}, schema: {schema}, role: {role}, warehouse: {warehouse}, connection start timestamp: {_startTime}");
            stopHeartBeatForThisSession();
            var closeSessionRequest = PrepareCloseSessionRequest();
            Task.Run(() => PostCloseSession(closeSessionRequest, restRequester));
            sessionToken = null;
        }

        internal async Task CloseAsync(CancellationToken cancellationToken)
        {
            // Nothing to do if the session is not open
            if (!IsEstablished()) return;
            logger.Debug($"Closing session with id: {sessionId}, user: {_user}, database: {database}, schema: {schema}, role: {role}, warehouse: {warehouse}, connection start timestamp: {_startTime}");
            stopHeartBeatForThisSession();

            var closeSessionRequest = PrepareCloseSessionRequest();

            logger.Debug($"Closing session async");
            var response = await restRequester.PostAsync<CloseResponse>(closeSessionRequest, cancellationToken).ConfigureAwait(false);
            if (!response.success)
            {
                logger.Error($"Failed to close session {sessionId}, error ignored. Code: {response.code} Message: {response.message}");
            }

            logger.Debug($"Session closed: {sessionId}");
            sessionToken = null;
        }

        private static void PostCloseSession(SFRestRequest closeSessionRequest, IRestRequester restRequester)
        {
            try
            {
                logger.Debug($"Closing session");
                var response = restRequester.Post<CloseResponse>(closeSessionRequest);
                if (!response.success)
                {
                    logger.Error($"Failed to close session: {closeSessionRequest.sid}, error ignored. Code: {response.code} Message: {response.message}");
                }

                logger.Debug($"Session closed: {closeSessionRequest.sid}");
            }
            catch (Exception)
            {
                logger.Error($"Failed to close session: {closeSessionRequest.sid}, because of exception.");
                throw;
            }
        }

        private SFRestRequest PrepareCloseSessionRequest()
        {
            var queryParams = new Dictionary<string, string>();
            queryParams[RestParams.SF_QUERY_SESSION_DELETE] = "true";
            queryParams[RestParams.SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            queryParams[RestParams.SF_QUERY_REQUEST_GUID] = Guid.NewGuid().ToString();

            return new SFRestRequest
            {
                Url = BuildUri(RestPath.SF_SESSION_PATH, queryParams),
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken),
                sid = sessionId
            };
        }

        internal bool IsEstablished() => sessionToken != null;

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
                RestTimeout = Timeout.InfiniteTimeSpan,
                _isLogin = true
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
                _isLogin = true
            };
        }

        internal SFRestRequest BuildTimeoutRestRequest(Uri uri, Object body, TimeSpan httpTimeout)
        {
            return new SFRestRequest()
            {
                jsonBody = body,
                Url = uri,
                authorizationToken = SF_AUTHORIZATION_BASIC,
                RestTimeout = connectionTimeout,
                HttpTimeout = httpTimeout,
                _isLogin = true
            };
        }

        internal void UpdateSessionParameterMap(List<NameValueParameter> parameterList)
        {
            logger.Debug("Update parameter map");
            // with HTAP parameter removal parameters might not returned
            // query response
            if (parameterList is null)
            {
                return;
            }

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
                if (keepAlive)
                {
                    startHeartBeatForThisSession();
                }
                else
                {
                    stopHeartBeatForThisSession();
                }
            }
            if ((!_disableQueryContextCache) &&
                (ParameterMap.ContainsKey(SFSessionParameter.QUERY_CONTEXT_CACHE_SIZE)))
            {
                string val = ParameterMap[SFSessionParameter.QUERY_CONTEXT_CACHE_SIZE].ToString();
                _queryContextCacheSize = Int32.Parse(val);
                _queryContextCache.SetCapacity(_queryContextCacheSize);
            }
        }

        internal void ClearQueryContextCache()
        {
            if (!_disableQueryContextCache)
            {
                _queryContextCache.ClearCacheSafely();
            }
        }

        internal void UpdateQueryContextCache(ResponseQueryContext queryContext)
        {
            if (!_disableQueryContextCache)
            {
                _queryContextCache.Update(queryContext);
            }
        }

        internal RequestQueryContext GetQueryContextRequest()
        {
            if (_disableQueryContextCache)
            {
                return null;
            }
            return _queryContextCache.GetQueryContextRequest();
        }

        internal void UpdateSessionProperties(QueryExecResponseData responseData)
        {
            // with HTAP session metadata removal database/schema might be not returned in query result
            UpdateSessionProperty(ref database, responseData.finalDatabaseName);
            UpdateSessionProperty(ref schema, responseData.finalSchemaName);
            UpdateSessionProperty(ref role, responseData.finalRoleName);
            UpdateSessionProperty(ref warehouse, responseData.finalWarehouseName);
        }

        internal void UpdateSessionProperty(ref string initialSessionValue, string finalSessionValue)
        {
            // with HTAP session metadata removal database/schema might be not returned in query result
            if (!string.IsNullOrEmpty(finalSessionValue))
            {
                bool quoted = false;
                string unquotedFinalValue = UnquoteJson(finalSessionValue, ref quoted);
                if (!string.IsNullOrEmpty(initialSessionValue))
                {
                    quoted |= initialSessionValue.StartsWith("\"");
                    if (!string.Equals(initialSessionValue, unquotedFinalValue, quoted ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase))
                    {
                        sessionPropertiesChanged = true;
                        initialSessionValue = unquotedFinalValue;
                    }
                }
                else // null session value gets populated and is not treated as a session property change
                {
                    initialSessionValue = unquotedFinalValue;
                }
            }
        }

        private static string UnquoteJson(string value, ref bool unquoted)
        {
            if (value is null)
                return value;
            unquoted = value.Length >= 4 && value.StartsWith("\\\"") && value.EndsWith("\\\"");
            return unquoted ? value.Replace("\\\"", "\"") : value;
        }

        internal bool SessionPropertiesChanged => sessionPropertiesChanged;

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
            if (IsEstablished())
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

        internal virtual bool IsNotOpen()
        {
            return _startTime == 0;
        }

        internal virtual bool IsExpired(TimeSpan timeout, long utcTimeInMillis)
        {
            var hasEverBeenOpened = !IsNotOpen();
            return hasEverBeenOpened && TimeoutHelper.IsExpired(_startTime, utcTimeInMillis, timeout);
        }

        internal long GetStartTime() => _startTime;

        internal void SetStartTime(long startTime)
        {
            _startTime = startTime;
        }

        internal void ReplaceAuthenticator(IAuthenticator authenticator)
        {
            this.authenticator = authenticator;
        }

        internal IAuthenticator GetAuthenticator() => authenticator;
    }
}
