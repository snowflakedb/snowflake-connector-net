/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Web;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;
using System.Text;

namespace Snowflake.Data.Core
{
    class SFStatement
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFStatement>();

        internal SFSession SfSession { get; set; }

        private const string SF_QUERY_CANCEL_PATH = "/queries/v1/abort-request";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private const int SF_SESSION_EXPIRED_CODE = 390112;

        private const int SF_QUERY_IN_PROGRESS = 333333;

        private const int SF_QUERY_IN_PROGRESS_ASYNC = 333334;

        private string _requestId;

        private readonly object _requestIdLock = new object();

        private readonly IRestRequester _restRequester;

        private CancellationTokenSource _timeoutTokenSource;
        
        // Merged cancellation token source for all cancellation signal. 
        // Cancel callback will be registered under token issued by this source.
        private CancellationTokenSource _linkedCancellationTokenSource;

        // Flag indicating if the SQL query is a regular query or a PUT/GET query
        internal bool isPutGetQuery = false;

        private MemoryStream putStream;

        public void SetPutStream(MemoryStream inStream)
        {
            putStream = inStream;
        }

        internal SFStatement(SFSession session)
        {
            SfSession = session;
            _restRequester = session.restRequester;
        }

        private void AssignQueryRequestId()
        {
            lock (_requestIdLock)
            {
                
                if (_requestId != null)
                {
                    logger.Info("Another query is running.");
                    throw new SnowflakeDbException(SFError.STATEMENT_ALREADY_RUNNING_QUERY);
                }

                _requestId = Guid.NewGuid().ToString();
            }
        }

        private void ClearQueryRequestId()
        {
            lock (_requestIdLock)
                _requestId = null;
        }

        private SFRestRequest BuildQueryRequest(string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            AssignQueryRequestId();

            TimeSpan startTime = DateTime.UtcNow - new DateTime(1970, 1, 1);
            string secondsSinceEpoch = Convert.ToInt64(startTime.TotalMilliseconds).ToString();
            Dictionary<string, string> parameters = new Dictionary<string, string>()
            {
                { RestParams.SF_QUERY_REQUEST_ID, _requestId },
                { RestParams.SF_QUERY_REQUEST_GUID, Guid.NewGuid().ToString() },
                { RestParams.SF_QUERY_START_TIME, secondsSinceEpoch },
            };

            var queryUri = SfSession.BuildUri(RestPath.SF_QUERY_PATH, parameters);

            QueryRequest postBody = new QueryRequest()
            {
                sqlText = sql,
                parameterBindings = bindings,
                describeOnly = describeOnly,
            };

            return new SFRestRequest
            {
                Url = queryUri,
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                serviceName = SfSession.ParameterMap.ContainsKey(SFSessionParameter.SERVICE_NAME)
                                ? (String)SfSession.ParameterMap[SFSessionParameter.SERVICE_NAME] : null,
                jsonBody = postBody,
                HttpTimeout = Timeout.InfiniteTimeSpan,
                RestTimeout = Timeout.InfiniteTimeSpan,
                isPutGet = isPutGetQuery
            };
        }

        private SFRestRequest BuildResultRequest(string resultPath)
        {
            var uri = SfSession.BuildUri(resultPath);
            return new SFRestRequest()
            {
                Url = uri,
                authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                HttpTimeout = Timeout.InfiniteTimeSpan,
                RestTimeout = Timeout.InfiniteTimeSpan
            };
        }

        private void CleanUpCancellationTokenSources()
        {
            if (_linkedCancellationTokenSource != null)
            {
                // This should also take care of cleaning up the cancellation callback that was registered.
                // https://github.com/microsoft/referencesource/blob/master/mscorlib/system/threading/CancellationTokenSource.cs#L552
                _linkedCancellationTokenSource.Dispose();
                _linkedCancellationTokenSource = null;
            }
            if (_timeoutTokenSource != null)
            {
                _timeoutTokenSource.Dispose();
                _timeoutTokenSource = null;
            }
        }

        private SFBaseResultSet BuildResultSet(QueryExecResponse response, CancellationToken cancellationToken)
        {
            if (response.success)
            {
                return new SFResultSet(response.data, this, cancellationToken);
            }

            throw new SnowflakeDbException(response.data.sqlState,
                response.code, response.message, response.data.queryId);
        }

        private void SetTimeout(int timeout)
        {
            this._timeoutTokenSource = timeout > 0 ? new CancellationTokenSource(timeout * 1000) :
                                                     new CancellationTokenSource(Timeout.InfiniteTimeSpan);
        }
        
        /// <summary>
        ///     Register cancel callback. Two factors: either external cancellation token passed down from upper
        ///     layer or timeout reached. Whichever comes first would trigger query cancellation.
        /// </summary>
        /// <param name="timeout">query timeout. 0 means no timeout</param>
        /// <param name="externalCancellationToken">cancellation token from upper layer</param>
        private void registerQueryCancellationCallback(int timeout, CancellationToken externalCancellationToken)
        {
            SetTimeout(timeout);
            _linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_timeoutTokenSource.Token,
                externalCancellationToken);
            if (!_linkedCancellationTokenSource.IsCancellationRequested)
            {
                _linkedCancellationTokenSource.Token.Register(() =>
                {
                    try
                    {
                        Cancel();
                    }
                    catch (Exception ex)
                    {
                        // Prevent an unhandled exception from being thrown
                        logger.Error("Unable to cancel query.", ex);
                    }
                });
            }
        }

        private bool RequestInProgress(BaseRestResponse r) =>
            r.code == SF_QUERY_IN_PROGRESS || r.code == SF_QUERY_IN_PROGRESS_ASYNC;

        private bool SessionExpired(BaseRestResponse r) => r.code == SF_SESSION_EXPIRED_CODE;

        internal async Task<SFBaseResultSet> ExecuteAsync(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly,
                                                          CancellationToken cancellationToken)
        {
            // Trim the sql query and check if this is a PUT/GET command
            string trimmedSql = TrimSql(sql);

            registerQueryCancellationCallback(timeout, cancellationToken);
            var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
            try
            {
                QueryExecResponse response = null;
                bool receivedFirstQueryResponse = false;
                while (!receivedFirstQueryResponse)
                {
                    response = await _restRequester.PostAsync<QueryExecResponse>(queryRequest, cancellationToken).ConfigureAwait(false);
                    if (SessionExpired(response))
                    {
                        SfSession.renewSession();
                        queryRequest.authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken);
                    }
                    else
                    {
                        receivedFirstQueryResponse = true;
                    }
                }

                var lastResultUrl = response.data?.getResultUrl;

                while (RequestInProgress(response) || SessionExpired(response))
                {
                    var req = BuildResultRequest(lastResultUrl);
                    response = await _restRequester.GetAsync<QueryExecResponse>(req, cancellationToken).ConfigureAwait(false);

                    if (SessionExpired(response))
                    {
                        logger.Info("Ping pong request failed with session expired, trying to renew the session.");
                        SfSession.renewSession();
                    }
                    else
                    {
                        lastResultUrl = response.data?.getResultUrl;
                    }
                }

                return BuildResultSet(response, cancellationToken);
            }
            catch
            {
                logger.Error("Query execution failed.");
                throw;
            }
            finally
            {
                CleanUpCancellationTokenSources();
                ClearQueryRequestId();
            }
        }
        
        internal SFBaseResultSet Execute(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            // Trim the sql query and check if this is a PUT/GET command
            string trimmedSql = TrimSql(sql);

            try
            {
                if (trimmedSql.StartsWith("PUT") || trimmedSql.StartsWith("GET"))
                {
                    isPutGetQuery = true;
                    PutGetExecResponse response =
                        ExecuteHelper<PutGetExecResponse, PutGetResponseData>(
                             timeout,
                             sql,
                             bindings,
                             describeOnly);

                    SFFileTransferAgent fileTransferAgent =
                        new SFFileTransferAgent(trimmedSql, SfSession, response.data, CancellationToken.None);

                    if (putStream != null)
                    {
                        fileTransferAgent.SetPutStream(putStream);
                    }

                    // Start the file transfer
                    fileTransferAgent.execute();

                    // Get the results of the upload/download
                    return fileTransferAgent.result();
                }
                else
                {
                    registerQueryCancellationCallback(timeout, CancellationToken.None);
                    var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
                    QueryExecResponse response = null;

                    bool receivedFirstQueryResponse = false;
                    while (!receivedFirstQueryResponse)
                    {
                        response = _restRequester.Post<QueryExecResponse>(queryRequest);
                        if (SessionExpired(response))
                        {
                            SfSession.renewSession();
                            queryRequest.authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken);
                        }
                        else
                        {
                            receivedFirstQueryResponse = true;
                        }
                    }

                    var lastResultUrl = response.data?.getResultUrl;
                    while (RequestInProgress(response) || SessionExpired(response))
                    {
                        var req = BuildResultRequest(lastResultUrl);
                        response = _restRequester.Get<QueryExecResponse>(req);

                        if (SessionExpired(response))
                        {
                            logger.Info("Ping pong request failed with session expired, trying to renew the session.");
                            SfSession.renewSession();
                        }
                        else
                        {
                            lastResultUrl = response.data?.getResultUrl;
                        }
                    }
                    return BuildResultSet(response, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                logger.Error("Query execution failed.", ex);
                throw;
            }
            finally
            {
                CleanUpCancellationTokenSources();
                ClearQueryRequestId();
            }
        }

        private SFRestRequest BuildCancelQueryRequest()
        {
            lock (_requestIdLock)
            {
                if (_requestId == null)
                    return null;
                Dictionary<string, string> parameters = new Dictionary<string, string>()
                {
                    { RestParams.SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString() },
                    { RestParams.SF_QUERY_REQUEST_GUID, Guid.NewGuid().ToString() },
                };
                var uri = SfSession.BuildUri(SF_QUERY_CANCEL_PATH, parameters);

                QueryCancelRequest postBody = new QueryCancelRequest()
                {
                    requestId = _requestId
                };

                return new SFRestRequest()
                {
                    Url = uri,
                    authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                    jsonBody = postBody
                };
            }
        }

        internal void Cancel()
        {
            SFRestRequest request = BuildCancelQueryRequest();
            if (request == null)
            {
                CleanUpCancellationTokenSources();
                return;
            }

            var response = _restRequester.Post<NullDataResponse>(request);

            if (response.success)
            {
                logger.Info("Query cancellation succeed");
            }
            else
            {
                logger.Warn("Query cancellation failed.");
            }
            CleanUpCancellationTokenSources();
        }

        /// <summary>
        /// Execute a sql query and return the response.
        /// </summary>
        /// <param name="timeout">The query timeout.</param>
        /// <param name="sql">The sql query.</param>
        /// <param name="bindings">Parameter bindings or null if no parameters.</param>
        /// <param name="describeOnly">Flag indicating if this will only return the metadata.</param>
        /// <returns>The response data.</returns>
        /// <exception>The http request fails or the response code is not succes</exception>
        internal T ExecuteHelper<T, U>(
            int timeout,
            string sql,
            Dictionary<string, BindingDTO> bindings,
            bool describeOnly)
            where T : BaseQueryExecResponse<U>
            where U : IQueryExecResponseData
        {
            registerQueryCancellationCallback(timeout, CancellationToken.None);
            var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
            try
            {
                T response = null;
                bool receivedFirstQueryResponse = false;
                while (!receivedFirstQueryResponse)
                {
                    response = _restRequester.Post<T>(queryRequest);
                    if (SessionExpired(response))
                    {
                        SfSession.renewSession();
                        queryRequest.authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken);
                    }
                    else
                    {
                        receivedFirstQueryResponse = true;
                    }
                }

                if (typeof(T) == typeof(QueryExecResponse))
                {
                    QueryExecResponse queryResponse = (QueryExecResponse)(object)response;
                    var lastResultUrl = queryResponse.data?.getResultUrl;
                    while (RequestInProgress(response) || SessionExpired(response))
                    {
                        var req = BuildResultRequest(lastResultUrl);
                        response = _restRequester.Get<T>(req);

                        if (SessionExpired(response))
                        {
                            logger.Info("Ping pong request failed with session expired, trying to renew the session.");
                            SfSession.renewSession();
                        }
                        else
                        {
                            lastResultUrl = queryResponse.data?.getResultUrl;
                        }
                    }
                }

                if (!response.success)
                {
                    throw new SnowflakeDbException(
                        response.data.sqlState,
                        response.code,
                        response.message,
                        response.data.queryId);
                }

                return response;
            }
            catch (Exception ex)
            {
                logger.Error("Query execution failed.", ex);
                throw;
            }
            finally
            {
                ClearQueryRequestId();
            }
        }

        /// <summary>
        /// Trim the query by removing spaces and comments at the beginning.
        /// </summary>
        /// <param name="originalSql">The original sql query.</param>
        /// <returns>The query without the blanks and comments at the beginning.</returns>
        private string TrimSql(string originalSql)
        {
            char[] sqlQueryBuf = originalSql.ToCharArray();
            var builder = new StringBuilder();

            // skip old c-style comment
            var idx = 0;
            var sqlQueryLen = sqlQueryBuf.Length;
            do
            {
                if (('/' == sqlQueryBuf[idx]) &&
                    (idx + 1 < sqlQueryLen) &&
                    ('*' == sqlQueryBuf[idx + 1]))
                {
                    // Search for the matching */
                    var matchingPos = originalSql.IndexOf("*/", idx + 2);
                    if (matchingPos >= 0)
                    {
                        // Found the comment closing, skip to after
                        idx = matchingPos + 2;
                    }
                }
                else if ((sqlQueryBuf[idx] == '-') &&
                         (idx + 1 < sqlQueryLen) &&
                         (sqlQueryBuf[idx + 1] == '-'))
                {
                    // Search for the new line
                    var newlinePos = originalSql.IndexOf("\n", idx + 2);

                    if (newlinePos >= 0)
                    {
                        // Found the new line, skip to after
                        idx = newlinePos + 1;
                    }
                }

                builder.Append(sqlQueryBuf[idx]);
                idx++;
            }
            while (idx < sqlQueryLen);

            var trimmedQuery = builder.ToString();
            logger.Debug("Trimmed query : " + trimmedQuery);

            return trimmedQuery;
        }
    }
}
