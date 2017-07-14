using System;
using System.Web;
using Newtonsoft.Json.Linq;
using Common.Logging;

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

        /// <summary>
        ///     Constructor 
        /// </summary>
        /// <param name="connectionString">A string in the form of "key1=value1;key2=value2"</param>
        internal SFSession(String connectionString)
        {
            restRequest = RestRequestImpl.Instance;
            properties = new SFSessionProperties(connectionString);
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
            AuthnRequestData data = new AuthnRequestData()
            {
                loginName = properties[SFSessionProperty.USER],
                password = properties[SFSessionProperty.PASSWORD],
                clientAppId = ".NET",
                clientAppVersion = "0.1",
                accountName = properties[SFSessionProperty.ACCOUNT]
            };

            // build request
            SFRestRequest loginRequest = new SFRestRequest();
            loginRequest.jsonBody = new AuthnRequest() { data = data };
            loginRequest.uri = uriBuilder.Uri;
            loginRequest.authorizationToken = SF_AUTHORIZATION_BASIC;

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
                throw new SFException();
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

            RenewSessionRequest renewSessionRequest = new RenewSessionRequest()
            {
                oldSessionToken = this.sessionToken,
                requestType = "RENEW"
            };

            SFRestRequest closeSessionRequest = new SFRestRequest();
            closeSessionRequest.jsonBody = null;
            closeSessionRequest.uri = uriBuilder.Uri;
            closeSessionRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, masterToken);

            JObject response = restRequest.post(closeSessionRequest);
            NullDataResponse deleteSessionResponse = response.ToObject<NullDataResponse>();
            if (!deleteSessionResponse.success)
            {
                throw new SFException();
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
            }
            else
            {
                throw new SFException();
            } 
        }
    }
}
