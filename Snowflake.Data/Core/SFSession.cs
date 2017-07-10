using System;
using System.Collections.Generic;
using System.Web;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Core
{
    class SFSession
    {
        private const string SF_SESSION_PATH = "/session";

        private const string SF_LOGIN_PATH = SF_SESSION_PATH + "/v1/login-request";

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
            RestRequest loginRequest = new RestRequest();
            loginRequest.jsonBody = new AuthnRequest() { data = data };
            loginRequest.uri = uriBuilder.Uri;
            loginRequest.authorizationToken = SF_AUTHORIZATION_BASIC;

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

            RestRequest closeSessionRequest = new RestRequest();
            closeSessionRequest.jsonBody = null;
            closeSessionRequest.uri = uriBuilder.Uri;
            closeSessionRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sessionToken);

            JObject response = restRequest.post(closeSessionRequest);
            DeleteSessionResponse deleteSessionResponse = response.ToObject<DeleteSessionResponse>();
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
            }
            else
            {
                throw new SFException();
            } 
        }
    }
}
