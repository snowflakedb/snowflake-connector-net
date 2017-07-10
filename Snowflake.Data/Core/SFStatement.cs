using System;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    class SFStatement
    {
        private SFSession sfSession;

        private const string SF_QUERY_PATH = "/queries/v1/query-request";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private IRestRequest restRequest;

        internal SFStatement(SFSession session)
        {
            this.sfSession = session;
            restRequest = RestRequestImpl.Instance;
        }

        internal SFBaseResultSet execute(string sql, ParameterBindings bindings, bool describeOnly)
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

            RestRequest queryRequest = new RestRequest();
            queryRequest.uri = uriBuilder.Uri;
            queryRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sfSession.sessionToken);
            queryRequest.jsonBody = postBody;

            JObject rawResponse = restRequest.post(queryRequest);
            QueryExecResponse execResponse = rawResponse.ToObject<QueryExecResponse>();
            
            if (execResponse.success)
            {
                return new SFResultSet(execResponse.data);
            }
            else
            {
                return null;
            }
        }
    }
}
