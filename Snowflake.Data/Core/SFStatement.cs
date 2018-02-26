/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Web;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Snowflake.Data.Client;
using Common.Logging;
using System.Threading;

namespace Snowflake.Data.Core
{
    class SFStatement
    {
        static private ILog logger = LogManager.GetLogger<SFStatement>();

        internal SFSession sfSession { get; set; }

        private const string SF_QUERY_PATH = "/queries/v1/query-request";

        private const string SF_QUERY_CANCEL_PATH = "/queries/v1/abort-request";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private const int SF_SESSION_EXPIRED_CODE = 390112;

        private const int SF_QUERY_IN_PROGRESS = 333333;

        private const int SF_QUERY_IN_PROGRESS_ASYNC = 333334;

        private string requestId = null;

        private IRestRequest restRequest;

        internal SFStatement(SFSession session)
        {
            this.sfSession = session;
            restRequest = RestRequestImpl.Instance;
        }

        internal SFBaseResultSet execute(string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            if (requestId != null)
            {
                logger.Info("Another query is running.");
                throw new SnowflakeDbException(SFError.STATEMENT_ALREADY_RUNNING_QUERY);
            }
            this.requestId = Guid.NewGuid().ToString(); 


            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = sfSession.properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = sfSession.properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(sfSession.properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_QUERY_PATH;

            var queryString = HttpUtility.ParseQueryString(string.Empty);
            queryString[SF_QUERY_REQUEST_ID] = requestId;
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
            queryRequest.httpRequestTimeout = Timeout.InfiniteTimeSpan;

            try
            {
                JObject rawResponse = restRequest.post(queryRequest);
                QueryExecResponse execResponse = rawResponse.ToObject<QueryExecResponse>();

                if (execResponse.code == SF_SESSION_EXPIRED_CODE)
                {
                    sfSession.renewSession();
                    this.requestId = null;
                    return this.execute(sql, bindings, describeOnly);
                }
                else if (execResponse.code == SF_QUERY_IN_PROGRESS ||
                         execResponse.code == SF_QUERY_IN_PROGRESS_ASYNC)
                {
                    logger.Info("Query execution in progress.");
                    bool isSessionRenewed = false;
                    string getResultUrl = null;
                    while (execResponse.code == SF_QUERY_IN_PROGRESS ||
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
                        getResultRequest.httpRequestTimeout = Timeout.InfiniteTimeSpan;

                        execResponse = null;
                        execResponse = restRequest.get(getResultRequest).ToObject<QueryExecResponse>();

                        if (execResponse.code == SF_SESSION_EXPIRED_CODE)
                        {
                            logger.Info("Ping pong request failed with session expired, trying to renew the session.");
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
                    SnowflakeDbException e = new SnowflakeDbException(
                        execResponse.data.sqlState, execResponse.code, execResponse.message, 
                        execResponse.data.queryId);
                    logger.Error("Query execution failed.", e);
                    throw e;
                }
            }
            finally
            {
                this.requestId = null;
            }
        }

        internal void cancel()
        {
            if (this.requestId == null)
            {
                logger.Info("No query to be cancelled.");
                return;
            }

            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = sfSession.properties[SFSessionProperty.SCHEME];
            uriBuilder.Host = sfSession.properties[SFSessionProperty.HOST];
            uriBuilder.Port = Int32.Parse(sfSession.properties[SFSessionProperty.PORT]);
            uriBuilder.Path = SF_QUERY_CANCEL_PATH;

            var queryString = HttpUtility.ParseQueryString(string.Empty);
            queryString[SF_QUERY_REQUEST_ID] = Guid.NewGuid().ToString();
            uriBuilder.Query = queryString.ToString();

            QueryCancelRequest postBody = new QueryCancelRequest()
            {
                requestId = this.requestId
            };

            SFRestRequest cancelRequest = new SFRestRequest();
            cancelRequest.uri = uriBuilder.Uri;
            cancelRequest.authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, sfSession.sessionToken);
            cancelRequest.jsonBody = postBody;

            NullDataResponse cancelResponse = restRequest.post(cancelRequest).ToObject<NullDataResponse>();
            if (cancelResponse.success)
            {
                logger.Info("Query cancellation succeed");
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException(
                    "", cancelResponse.code, cancelResponse.message, "");
                logger.Error("Query cancellation failed.", e);
                throw e;
            }
            
        }

        internal void setQueryTimeoutBomb(int timeout)
        {
            System.Threading.Timer timer = null;
            timer = new System.Threading.Timer(
                (object state) =>
                {
                    this.cancel();
                    timer.Dispose();
                },
                null, TimeSpan.FromSeconds(timeout), TimeSpan.FromMilliseconds(-1));
        }
    }
}
