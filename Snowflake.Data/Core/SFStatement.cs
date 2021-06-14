/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Web;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

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
        
        // Merged cancellation token source for all canellation signal. 
        // Cancel callback will be registered under token issued by this source.
        private CancellationTokenSource _linkedCancellationTokenSouce;

        internal SFStatement(SFSession session, IRestRequester rest)
        {
            SfSession = session;
            _restRequester = rest;
        }

        internal SFStatement(SFSession session) : this(session, RestRequester.Instance)
        { }

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

            return new SFRestRequest(SfSession.InsecureMode)
            {
                Url = queryUri,
                authorizationToken = string.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                serviceName = SfSession.ParameterMap.ContainsKey(SFSessionParameter.SERVICE_NAME)
                                ? (String)SfSession.ParameterMap[SFSessionParameter.SERVICE_NAME] : null,
                jsonBody = postBody,
                HttpTimeout = Timeout.InfiniteTimeSpan,
                RestTimeout = Timeout.InfiniteTimeSpan
            };
        }

        private SFRestRequest BuildResultRequest(string resultPath)
        {
            var uri = SfSession.BuildUri(resultPath);
            return new SFRestRequest(SfSession.InsecureMode)
            {
                Url = uri,
                authorizationToken = String.Format(SF_AUTHORIZATION_SNOWFLAKE_FMT, SfSession.sessionToken),
                HttpTimeout = Timeout.InfiniteTimeSpan,
                RestTimeout = Timeout.InfiniteTimeSpan
            };
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
            _linkedCancellationTokenSouce = CancellationTokenSource.CreateLinkedTokenSource(_timeoutTokenSource.Token,
                externalCancellationToken);
            if (!_linkedCancellationTokenSouce.IsCancellationRequested)
            {
                _linkedCancellationTokenSouce.Token.Register(() => 
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

        private bool RequestInProgress(QueryExecResponse r) =>
            r.code == SF_QUERY_IN_PROGRESS || r.code == SF_QUERY_IN_PROGRESS_ASYNC;

        private bool SessionExpired(QueryExecResponse r) => r.code == SF_SESSION_EXPIRED_CODE;

        internal async Task<SFBaseResultSet> ExecuteAsync(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly,
                                                          CancellationToken cancellationToken)
        {
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
                ClearQueryRequestId();
            }
        }
        
        internal SFBaseResultSet Execute(int timeout, string sql, Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            registerQueryCancellationCallback(timeout, CancellationToken.None);
            var queryRequest = BuildQueryRequest(sql, bindings, describeOnly);
            try
            {
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

                return new SFRestRequest(SfSession.InsecureMode)
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
                return;

            var response = _restRequester.Post<NullDataResponse>(request);

            if (response.success)
            {
                logger.Info("Query cancellation succeed");
            }
            else
            {
                logger.Warn("Query cancellation failed.");
            }
        }
        
    }
}
