using System;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    class SFStatement
    {
        internal SFSession sfSession { get; set; }

        private const string SF_QUERY_PATH = "/queries/v1/query-request";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private const int SF_SESSION_EXPIRED_CODE = 390112;

        private const int SF_QUERY_IN_PROGRESS = 333333;

        private const int SF_QUERY_IN_PROGRESS_ASYNC = 333334;

        private IRestRequest restRequest;

        internal SFStatement(SFSession session)
        {
            this.sfSession = session;
            restRequest = RestRequestImpl.Instance;
        }

        internal SFBaseResultSet execute(string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = sfSession.properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = sfSession.properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(sfSession.properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_QUERY_PATH;

            var queryString = HttpUtility.ParseQueryString(string.Empty);
            queryString[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            uriBuilder.Query = queryString.ToString();

            QueryRequest postBody = new QueryRequest()
            {
                sqlText = sql,
                parameterBindings = bindings,
                describeOnly = describeOnly,
            };

            SFRestRequest queryRequest = new SFRestRequest();
            queryRequest.uri = uriBuilder.Uri;
            queryRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sfSession.sessionToken);
            queryRequest.jsonBody = postBody;

            JObject rawResponse = restRequest.post(queryRequest);
            QueryExecResponse execResponse = rawResponse.ToObject<QueryExecResponse>();
            
            if (execResponse.code == SF_SESSION_EXPIRED_CODE)
            {
                sfSession.renewSession();
                this.execute(sql, bindings, describeOnly);
            }
            else if (execResponse.code == SF_QUERY_IN_PROGRESS ||
                     execResponse.code == SF_QUERY_IN_PROGRESS_ASYNC)
            {
                bool isSessionRenewed = false;
                string getResultUrl = null;
                while(execResponse.code == SF_QUERY_IN_PROGRESS ||
                      execResponse.code == SF_QUERY_IN_PROGRESS_ASYNC)
                {
                    if (!isSessionRenewed)
                    {
                        getResultUrl = execResponse.data.getResultUrl;
                    }

                    UriBuilder getResultUriBuilder = new UriBuilder();
                    getResultUriBuilder.Scheme = sfSession.properties[SFSessionProperty.SCHEME];
                    getResultUriBuilder.Host = sfSession.properties[SFSessionProperty.HOST];
                    getResultUriBuilder.Port = Int32.Parse(sfSession.properties[SFSessionProperty.PORT]);
                    getResultUriBuilder.Path = getResultUrl;

                    SFRestRequest getResultRequest = new SFRestRequest()
                    {
                        uri = getResultUriBuilder.Uri,
                        authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sfSession.sessionToken)
                    };

                    execResponse = null;
                    execResponse = restRequest.get(getResultRequest).ToObject<QueryExecResponse>();
                    
                    if (execResponse.code == SF_SESSION_EXPIRED_CODE)
                    {
                        sfSession.renewSession();
                        isSessionRenewed = true;
                    }
                    else
                    {
                        isSessionRenewed = false;
                    }
                }
            }

            if (execResponse.success)
            {
                return new SFResultSet(execResponse.data, this);
            }
            else
            {
                throw new SnowflakeDbException(execResponse.data.sqlState, execResponse.code, execResponse.message);
            }
        }
    }
}
