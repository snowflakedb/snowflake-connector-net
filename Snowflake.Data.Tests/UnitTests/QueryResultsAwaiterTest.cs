using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class QueryResultsAwaiterTest
    {
        private const string ConnectionString = "account=test;user=test;password=test;poolingEnabled=false";
        private static readonly string ValidQueryId = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        [SFFact]
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
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true).ConfigureAwait(false)).ConfigureAwait(false);

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [SFFact]
        public async Task TestCancellationSendsAbortRequestDuringPollingAsync()
        {
            // Arrange: simulates the repro scenario where a query is running (status=RUNNING)
            // and the token is cancelled while polling for results
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();

            // Cancel after the first status check returns Running,
            // so the subsequent Task.Delay throws TaskCanceledException
            mockRequester.OnGetQueryStatusResponse = () => cts.Cancel();

            // Act
            try
            {
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true).ConfigureAwait(false);
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

        [SFFact]
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
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task.ConfigureAwait(false)).ConfigureAwait(false);

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [SFFact]
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
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task.ConfigureAwait(false)).ConfigureAwait(false);

            // Assert
            Assert.True(mockRequester.CancelRequestSent,
                "Expected a SYSTEM$CANCEL_QUERY command to be sent when cancellation is requested during sync polling");
            Assert.Equal(ValidQueryId, mockRequester.CancelledQueryId);
        }

        [SFFact]
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
                await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, cts.Token, true).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request should not have succeeded since the mock was configured to throw");
        }

        [SFFact]
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
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task.ConfigureAwait(false)).ConfigureAwait(false);
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request should not have succeeded since the mock was configured to throw");
        }

        [SFFact]
        public async Task TestPollingCompletesWhenQueryFinishes()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            mockRequester.EnqueueStatus("RUNNING", "RUNNING", "SUCCESS");
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 0, 0, 0 }));

            await awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, CancellationToken.None, true).ConfigureAwait(false);

            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent when query completes normally");
        }

        [SFFact]
        public async Task TestPollingCompletesWhenQueryFinishesSync()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            mockRequester.EnqueueStatus("RUNNING", "RUNNING", "SUCCESS");
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 0, 0, 0 }));

            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, ValidQueryId, CancellationToken.None, false);
            await task.ConfigureAwait(false);

            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent when query completes normally");
        }

        [SFFact]
        public void TestNoCancelRequestWhenNotCancelled()
        {
            var mockRequester = new MockRestRequesterForQueryCancellation();
            Assert.False(mockRequester.CancelRequestSent,
                "No cancel request should be sent before any operations");
            Assert.Null(mockRequester.CancelledQueryId);
        }

        [SFTheory]
        [InlineData("'; DO SOMETHING ELSE; --")]
        [InlineData("not-a-uuid")]
        [InlineData("")]
        [InlineData("a1b2c3d4-e5f6-7890-abcd-ef123456789")] // too short
        [InlineData("a1b2c3d4-e5f6-7890-abcd-ef12345678901")] // too long
        public async Task TestInvalidQueryIdDoesNotSendCancelRequest(string invalidQueryId)
        {
            // Arrange
            var mockRequester = new MockRestRequesterForQueryCancellation();
            var conn = new MockSnowflakeDbConnection(mockRequester);
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var awaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(1, new[] { 1 }));
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act - cancellation triggers AbortQuery which validates the query ID
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await awaiter.RetryUntilQueryResultIsAvailable(conn, invalidQueryId, cts.Token, true).ConfigureAwait(false)).ConfigureAwait(false);

            // Assert - no SQL command should have been sent
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request must not be sent for an invalid query ID.");
        }

        [SFTheory]
        [InlineData("'; DO SOMETHING ELSE; --")]
        [InlineData("not-a-uuid")]
        [InlineData("")]
        public async Task TestInvalidQueryIdDoesNotSendCancelRequestSync(string invalidQueryId)
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
            var task = awaiter.RetryUntilQueryResultIsAvailable(conn, invalidQueryId, cts.Token, false);
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task.ConfigureAwait(false)).ConfigureAwait(false);

            // Assert
            Assert.False(mockRequester.CancelRequestSent,
                "Cancel request must not be sent for an invalid query ID.");
        }
    }
}
