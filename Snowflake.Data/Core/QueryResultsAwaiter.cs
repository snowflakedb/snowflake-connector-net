using Snowflake.Data.Log;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Text.RegularExpressions;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    internal class QueryResultsRetryConfig
    {
        private const int DefaultAsyncNoDataMaxRetry = 24;

        private readonly int[] _defaultAsyncRetryPattern = { 1, 1, 2, 3, 4, 8, 10 };

        internal readonly int _asyncNoDataMaxRetry;

        internal readonly int[] _asyncRetryPattern;

        internal QueryResultsRetryConfig()
        {
            _asyncNoDataMaxRetry = DefaultAsyncNoDataMaxRetry;
            _asyncRetryPattern = _defaultAsyncRetryPattern;
        }

        internal QueryResultsRetryConfig(int asyncNoDataMaxRetry, int[] asyncRetryPattern)
        {
            _asyncNoDataMaxRetry = asyncNoDataMaxRetry;
            _asyncRetryPattern = asyncRetryPattern;
        }
    }

    internal class QueryResultsAwaiter
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<QueryResultsAwaiter>();

        private static readonly Regex UuidRegex = new Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

        private QueryResultsRetryConfig _queryResultsRetryConfig;

        internal static readonly QueryResultsAwaiter Instance = new QueryResultsAwaiter();

        internal QueryResultsAwaiter()
        {
            _queryResultsRetryConfig = new QueryResultsRetryConfig();
        }

        internal QueryResultsAwaiter(QueryResultsRetryConfig queryResultsRetryConfig)
        {
            _queryResultsRetryConfig = queryResultsRetryConfig;
        }

        internal QueryStatus GetQueryStatus(SnowflakeDbConnection connection, string queryId)
        {
            if (UuidRegex.IsMatch(queryId))
            {
                var sfStatement = new SFStatement(connection.SfSession);
                return sfStatement.GetQueryStatus(queryId);
            }
            else
            {
                var errorMessage = $"The given query id {queryId} is not valid uuid";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        internal async Task<QueryStatus> GetQueryStatusAsync(SnowflakeDbConnection connection, string queryId, CancellationToken cancellationToken)
        {
            if (UuidRegex.IsMatch(queryId))
            {
                var sfStatement = new SFStatement(connection.SfSession);
                return await sfStatement.GetQueryStatusAsync(queryId, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var errorMessage = $"The given query id {queryId} is not valid uuid";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        /// <summary>
        /// Checks query status until it is done executing.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="queryId"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="isAsync"></param>
        internal async Task RetryUntilQueryResultIsAvailable(SnowflakeDbConnection connection, string queryId, CancellationToken cancellationToken, bool isAsync)
        {
            int retryPatternPos = 0;
            int noDataCounter = 0;

            QueryStatus status;
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    s_logger.Debug("Cancellation requested for getting results from query id");
                    cancellationToken.ThrowIfCancellationRequested();
                }

                status = isAsync ? await GetQueryStatusAsync(connection, queryId, cancellationToken) : GetQueryStatus(connection, queryId);

                if (!QueryStatusExtensions.IsStillRunning(status))
                {
                    return;
                }

                // Timeout based on query status retry rules
                if (isAsync)
                {
                    await Task.Delay(TimeSpan.FromSeconds(_queryResultsRetryConfig._asyncRetryPattern[retryPatternPos]), cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Thread.Sleep(TimeSpan.FromSeconds(_queryResultsRetryConfig._asyncRetryPattern[retryPatternPos]));
                }

                // If no data, increment the no data counter
                if (status == QueryStatus.NoData)
                {
                    noDataCounter++;

                    // Check if retry for no data is exceeded
                    if (noDataCounter > _queryResultsRetryConfig._asyncNoDataMaxRetry)
                    {
                        var errorMessage = "Max retry for no data is reached";
                        s_logger.Error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                }

                if (retryPatternPos < _queryResultsRetryConfig._asyncRetryPattern.Length - 1)
                {
                    retryPatternPos++;
                }
            }
        }
    }
}
