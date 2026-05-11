using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Mock;

namespace Snowflake.Data.Tests.UnitTests
{
    class QueryResultsAwaiterTest
    {
        private const string ConnectionString = "account=test;user=test;password=test;poolingEnabled=false";
        private static readonly string ValidQueryId = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        [Fact]
        public async Task TestCancellationSendsAbortRequestForPreCancelledToken()
        {
            // Arrange
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true));

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [Fact]
        public async Task TestCancellationSendsAbortRequestDuringPollingAsync()
        {
            // Arrange: simulates the repro scenario where a query is running (status=RUNNING)
            // and the token is cancelled while polling for results
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            await conn.OpenAsync(CancellationToken.None);

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();

            // Cancel after the first status check returns Running,
            // so the subsequent Task.Delay throws TaskCanceledException
            mockRequester.OnGetQueryStatusResponse = () => cts.Cancel();

            // Act
            try
            {
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true);
                Assert.Fail("Expected an OperationCanceledException to be thrown");
            }
            catch (OperationCanceledException)
            {
                // expected
            }

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested during polling");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [Fact]
        public async Task TestCancellationSendsAbortRequestSync()
        {
            // Arrange
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, false);
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [Fact]
        public async Task TestCancellationSendsAbortRequestDuringPollingSync()
        {
            // Arrange: sync polling where the token is cancelled after the first status check
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 0 }));
            var cts = new CancellationTokenSource();

            mockRequester.OnGetQueryStatusResponse = () => cts.Cancel();

            // Act
            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, false);
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested during sync polling");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [Fact]
        public async Task TestAbortQueryFailureDoesNotSuppressCancellationException()
        {
            // Arrange: cancel request itself throws, but OperationCanceledException should still propagate
            var mockRequester = new MockRestRequesterForQueryCancellation { ThrowOnCancel = true };
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true));
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request should not have succeeded since the mock was configured to throw");
        }

        [Fact]
        public async Task TestAbortQueryFailureDoesNotSuppressCancellationExceptionSync()
        {
            // Arrange: same as above but for the sync (isAsync=false) path
            var mockRequester = new MockRestRequesterForQueryCancellation { ThrowOnCancel = true };
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, false);
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request should not have succeeded since the mock was configured to throw");
        }

        [Fact]
        public async Task TestPollingCompletesWhenQueryFinishes()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            mockRequester.EnqueueStatus("RUNNING", "RUNNING", "SUCCESS");
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            await conn.OpenAsync(CancellationToken.None);

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 0, 0, 0 }));

            await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, CancellationToken.None, true);

            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent when query completes normally");
        }

        [Fact]
        public async Task TestPollingCompletesWhenQueryFinishesSync()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            mockRequester.EnqueueStatus("RUNNING", "RUNNING", "SUCCESS");
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 0, 0, 0 }));

            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, CancellationToken.None, false);
            await task;

            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent when query completes normally");
        }

        [Fact]
        public void TestNoCancelRequestWhenNotCancelled()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent before any operations");
            Assert.Null(mockRequester.CancelledQueryId);
        }
    }
}
