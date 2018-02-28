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
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    class SFStatement
    {
        static private ILog logger = LogManager.GetLogger<SFStatement>();

        internal SFSession SfSession { get; set; }

        private const string SF_QUERY_PATH = "/queries/v1/query-request";

        private const string SF_QUERY_CANCEL_PATH = "/queries/v1/abort-request";

        private const string SF_QUERY_REQUEST_ID = "requestId";

        private const string SF_AUTHORIZATION_SNOWFLAKE_FMT = "Snowflake Token=\"{0}\"";

        private const int SF_SESSION_EXPIRED_CODE = 390112;

        private const int SF_QUERY_IN_PROGRESS = 333333;

        private const int SF_QUERY_IN_PROGRESS_ASYNC = 333334;

        private string _requestId;

        private readonly object _requestIdLock = new object();

        private readonly IRestRequest _restRequest;

        private CancellationTokenSource _timeoutTokenSource;

        internal SFStatement(SFSession session)
        {
            SfSession = session;
            _restRequest = RestRequestImpl.Instance;
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
            _timeoutTokenSource?.Cancel();

            lock (_requestIdLock)
                _requestId = null;
        }

        private SFRestRequest BuildQueryRequest(string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            AssignQueryRequestId();

            var queryUri = SfSession.BuildUri(SF_QUERY_PATH,
                new Dictionary<string, string>() {{SF_QUERY_REQUEST_ID, _requestId}});

            QueryRequest postBody = new QueryRequest()
            {
                sqlText = sql,
                parameterBindings = bindings,
                describeOnly = describeOnly,
            };

            return new SFRestRequest
            {
                uri = queryUri,
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                jsonBody = postBody,
                httpRequestTimeout = Timeout.InfiniteTimeSpan
            };
        }

        private SFRestRequest BuildResultRequest(string resultPath)
        {
            var uri = SfSession.BuildUri(resultPath);
            return new SFRestRequest()
            {
                uri = uri,
                authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                httpRequestTimeout = Timeout.InfiniteTimeSpan
            };
        }

        private SFBaseResultSet BuildResultSet(QueryExecResponse response)
        {
            if (response.success)
            {
                return new SFResultSet(response.data, this);
            }

            throw new SnowflakeDbException(response.data.sqlState,
                response.code, response.message, response.data.queryId);
        }

        private void SetTimeout(int timeout)
        {
            if (timeout > 0)
            {
                _timeoutTokenSource = new CancellationTokenSource();
                Task.Delay(TimeSpan.FromSeconds(timeout), _timeoutTokenSource.Token)
                    .ContinueWith(async (t) => await CancelAsync(), _timeoutTokenSource.Token);
            }
        }

        private bool RequestInProgress(QueryExecResponse r) =>
            r.code == SF_QUERY_IN_PROGRESS || r.code == SF_QUERY_IN_PROGRESS_ASYNC;

        private bool SessionExpired(QueryExecResponse r) => r.code == SF_SESSION_EXPIRED_CODE;

        
        internal async Task<SFBaseResultSet> ExecuteAsync(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
            try
            {
                SetTimeout(timeout);
                var response = await _restRequest.PostAsync<QueryExecResponse>(queryRequest);
                if (SessionExpired(response))
                {
                    SfSession.renewSession();
                    ClearQueryRequestId();
                    return await ExecuteAsync(timeout, sql, bindings, describeOnly);
                }

                var lastResultUrl = response.data?.getResultUrl;

                while (RequestInProgress(response) || SessionExpired(response))
                {
                    var req = BuildResultRequest(lastResultUrl);
                    response = await _restRequest.GetAsync<QueryExecResponse>(req);

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

                return BuildResultSet(response);
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
        
        internal SFBaseResultSet Execute(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
            try
            {
                SetTimeout(timeout);

                var response = _restRequest.Post<QueryExecResponse>(queryRequest);
                if (SessionExpired(response))
                {
                    SfSession.renewSession();
                    ClearQueryRequestId();
                    return Execute(timeout, sql, bindings, describeOnly);
                }

                var lastResultUrl = response.data?.getResultUrl;
                while (RequestInProgress(response) || SessionExpired(response))
                {
                    var req = BuildResultRequest(lastResultUrl);
                    response = _restRequest.Get<QueryExecResponse>(req);

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

                return BuildResultSet(response);
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

        private SFRestRequest BuildCancelQueryRequest()
        {
            lock (_requestIdLock)
            {
                if (_requestId == null)
                    return null;

                var uri = SfSession.BuildUri(SF_QUERY_CANCEL_PATH,
                    new Dictionary<string, string> {{SF_QUERY_REQUEST_ID, Guid.NewGuid().ToString()}});

                QueryCancelRequest postBody = new QueryCancelRequest()
                {
                    requestId = _requestId
                };

                return new SFRestRequest()
                {
                    uri = uri,
                    authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                    jsonBody = postBody
                };
            }
        }

        internal async Task CancelAsync()
        {
            SFRestRequest request = BuildCancelQueryRequest();
            if (request == null)
                return;

            var response = await _restRequest.PostAsync<NullDataResponse>(request);

            if(response.success)
            {
                logger.Info("Query cancellation succeed");
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException(
                    "", response.code, response.message, "");
                logger.Error("Query cancellation failed.", e);
                throw e;
            }

        }
        

        internal void Cancel()
        {
            SFRestRequest request = BuildCancelQueryRequest();
            if (request == null)
                return;

            var response = _restRequest.Post<NullDataResponse>(request);

            if (response.success)
            {
                logger.Info("Query cancellation succeed");
            }
            else
            {
                SnowflakeDbException e = new SnowflakeDbException(
                    "", response.code, response.message, "");
                logger.Error("Query cancellation failed.", e);
                throw e;
            }
        }
        
    }
}
