using System.Collections.Generic;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Xunit;
using System;
using System.Threading.Tasks;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    /**
     * Mock rest request test
     */
    public class SFStatementTest
    {
        // Mock test for session token renew
        [SFFact]
        public void TestSessionRenew()
        {
            var restRequester = new Mock.MockRestSessionExpired();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var resultSet = statement.Execute(StatementContext.Default with { CommandText = "select 1" }, null);
            Assert.True(resultSet.Next());
            Assert.Equal("1", resultSet.GetString(0));
            Assert.Equal("new_session_token", sfSession.sessionToken);
            Assert.Equal("new_master_token", sfSession.masterToken);
            Assert.Equal(restRequester.FirstTimeRequestID, restRequester.SecondTimeRequestID);
        }

        [SFFact]
        public void TestSessionRenewGetResultWithId()
        {
            var restRequester = new Mock.MockRestSessionExpired();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var resultSet = statement.GetResultWithId("mockId");
            Assert.True(resultSet.Next());
            Assert.Equal("abc", resultSet.GetString(0));
            Assert.Equal("new_session_token", sfSession.sessionToken);
            Assert.Equal("new_master_token", sfSession.masterToken);
        }

        [SFFact]
        public void TestSessionRenewGetResultWithIdOnlyRetries3Times()
        {
            var restRequester = new Mock.MockRestSessionExpired();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.GetResultWithId("retryId"));
            Assert.Equal(thrown.ErrorCode, Mock.MockRestSessionExpired.SESSION_EXPIRED_CODE);
        }

        [SFFact]
        public async Task TestSessionRenewGetResultWithIdAsync()
        {
            var restRequester = new Mock.MockRestSessionExpired();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var resultSet = await statement.GetResultWithIdAsync("mockId", CancellationToken.None).ConfigureAwait(false);
            Assert.True(resultSet.Next());
            Assert.Equal("abc", resultSet.GetString(0));
            Assert.Equal("new_session_token", sfSession.sessionToken);
            Assert.Equal("new_master_token", sfSession.masterToken);
        }

        [SFFact]
        public async Task TestSessionRenewGetResultWithIdOnlyRetries3TimesAsync()
        {
            var restRequester = new Mock.MockRestSessionExpired();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () => await statement.GetResultWithIdAsync("retryId", CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Equal(thrown.ErrorCode, Mock.MockRestSessionExpired.SESSION_EXPIRED_CODE);
        }

        // Mock test for session renew during query execution
        [SFFact]
        public void TestSessionRenewDuringQueryExec()
        {
            var restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var resultSet = statement.Execute(StatementContext.Default with { CommandText = "select 1" }, null);
            Assert.True(resultSet.Next());
            Assert.Equal("1", resultSet.GetString(0));
        }

        // Mock test for Service Name
        // The Mock requester would take in the X-Snowflake-Service header in the request
        // and append a character 'a' at the end, send back as SERVICE_NAME parameter
        // This test is to assure that SETVICE_NAME parameter would be upgraded to the session
        [SFFact]
        public void TestServiceName()
        {
            var restRequester = new Mock.MockServiceName();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var expectServiceName = Mock.MockServiceName.INIT_SERVICE_NAME;
            Assert.Equal(expectServiceName, sfSession.ParameterMap[SFSessionParameter.SERVICE_NAME]);
            for (var i = 0; i < 5; i++)
            {
                var statement = new SFStatement(sfSession);
                var resultSet = statement.Execute(StatementContext.Default with { CommandText = "SELECT 1" }, null);
                expectServiceName += "a";
                Assert.Equal(expectServiceName, sfSession.ParameterMap[SFSessionParameter.SERVICE_NAME]);
            }
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a block comment
        /// </summary>
        [SFFact]
        public void TestTrimSqlBlockComment()
        {
            const string SqlSource = "/*comment*/select 1/*comment*/";
            const string SqlExpected = "select 1";

            Assert.Equal(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a multiline block comment
        /// </summary>
        [SFFact]
        public void TestTrimSqlBlockCommentMultiline()
        {
            const string SqlSource = "/*comment\r\ncomment*/select 1/*comment\r\ncomment*/";
            const string SqlExpected = "select 1";

            Assert.Equal(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment
        /// </summary>
        [SFFact]
        public void TestTrimSqlLineComment()
        {
            const string SqlSource = "--comment\r\nselect 1\r\n--comment";
            const string SqlExpected = "select 1\r\n--comment";

            Assert.Equal(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment with a closing newline
        /// </summary>
        [SFFact]
        public void TestTrimSqlLineCommentWithClosingNewline()
        {
            const string SqlSource = "--comment\r\nselect 1\r\n--comment\r\n";
            const string SqlExpected = "select 1";

            Assert.Equal(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        [SFTheory]
        [InlineData("running", QueryStatus.Running)]
        [InlineData("RUNNING", QueryStatus.Running)]
        [InlineData("resuming_warehouse", QueryStatus.ResumingWarehouse)]
        [InlineData("RESUMING_WAREHOUSE", QueryStatus.ResumingWarehouse)]
        [InlineData("queued", QueryStatus.Queued)]
        [InlineData("QUEUED", QueryStatus.Queued)]
        [InlineData("queued_reparing_warehouse", QueryStatus.QueuedReparingWarehouse)]
        [InlineData("QUEUED_REPARING_WAREHOUSE", QueryStatus.QueuedReparingWarehouse)]
        [InlineData("no_data", QueryStatus.NoData)]
        [InlineData("NO_DATA", QueryStatus.NoData)]
        [InlineData("aborting", QueryStatus.Aborting)]
        [InlineData("ABORTING", QueryStatus.Aborting)]
        [InlineData("success", QueryStatus.Success)]
        [InlineData("SUCCESS", QueryStatus.Success)]
        [InlineData("failed_with_error", QueryStatus.FailedWithError)]
        [InlineData("FAILED_WITH_ERROR", QueryStatus.FailedWithError)]
        [InlineData("aborted", QueryStatus.Aborted)]
        [InlineData("ABORTED", QueryStatus.Aborted)]
        [InlineData("failed_with_incident", QueryStatus.FailedWithIncident)]
        [InlineData("FAILED_WITH_INCIDENT", QueryStatus.FailedWithIncident)]
        [InlineData("disconnected", QueryStatus.Disconnected)]
        [InlineData("DISCONNECTED", QueryStatus.Disconnected)]
        [InlineData("restarted", QueryStatus.Restarted)]
        [InlineData("RESTARTED", QueryStatus.Restarted)]
        [InlineData("blocked", QueryStatus.Blocked)]
        [InlineData("BLOCKED", QueryStatus.Blocked)]
        public void TestGetQueryStatusByStringValue(string stringValue, QueryStatus expectedStatus)
        {
            Assert.Equal(expectedStatus, QueryStatusExtensions.GetQueryStatusByStringValue(stringValue));
        }

        [SFTheory]
        [InlineData("UNKNOWN")]
        [InlineData("RANDOM_STATUS")]
        [InlineData("aBcZyX")]
        public void TestGetQueryStatusByStringValueThrowsErrorForUnknownStatus(string stringValue)
        {
            var thrown = Assert.Throws<Exception>(() => QueryStatusExtensions.GetQueryStatusByStringValue(stringValue));
            Assert.Contains("The query status returned by the server is not recognized", thrown.Message);
        }

        [SFTheory]
        [InlineData(QueryStatus.Running, true)]
        [InlineData(QueryStatus.ResumingWarehouse, true)]
        [InlineData(QueryStatus.Queued, true)]
        [InlineData(QueryStatus.QueuedReparingWarehouse, true)]
        [InlineData(QueryStatus.NoData, true)]
        [InlineData(QueryStatus.Aborting, false)]
        [InlineData(QueryStatus.Success, false)]
        [InlineData(QueryStatus.FailedWithError, false)]
        [InlineData(QueryStatus.Aborted, false)]
        [InlineData(QueryStatus.FailedWithIncident, false)]
        [InlineData(QueryStatus.Disconnected, false)]
        [InlineData(QueryStatus.Restarted, false)]
        [InlineData(QueryStatus.Blocked, false)]
        public void TestIsStillRunning(QueryStatus status, bool expectedResult)
        {
            Assert.Equal(expectedResult, QueryStatusExtensions.IsStillRunning(status));
        }

        [SFTheory]
        [InlineData(QueryStatus.Aborting, true)]
        [InlineData(QueryStatus.FailedWithError, true)]
        [InlineData(QueryStatus.Aborted, true)]
        [InlineData(QueryStatus.FailedWithIncident, true)]
        [InlineData(QueryStatus.Disconnected, true)]
        [InlineData(QueryStatus.Blocked, true)]
        [InlineData(QueryStatus.Running, false)]
        [InlineData(QueryStatus.ResumingWarehouse, false)]
        [InlineData(QueryStatus.Queued, false)]
        [InlineData(QueryStatus.QueuedReparingWarehouse, false)]
        [InlineData(QueryStatus.NoData, false)]
        [InlineData(QueryStatus.Success, false)]
        [InlineData(QueryStatus.Restarted, false)]
        public void TestIsAnError(QueryStatus status, bool expectedResult)
        {
            Assert.Equal(expectedResult, QueryStatusExtensions.IsAnError(status));
        }

        [SFFact]
        public void TestHandleNullDataForFailedResponse()
        {
            // arrange
            var response = new QueryExecResponse
            {
                success = false,
                code = 500,
                message = "internal error"
            };
            var session = new SFSession("account=myAccount;password=myPassword;user=myUser;db=myDB", new SessionPropertiesContext());
            var statement = new SFStatement(session);

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.BuildResultSet(response, CancellationToken.None));

            // assert
            Assert.Equal("Error: internal error SqlState: , VendorCode: 500, QueryId: ", thrown.Message);
        }

        [SFFact]
        public void TestBuildResultSetMergesQccOnFailedResponse()
        {
            var queryContext = new ResponseQueryContext
            {
                Entries = new List<ResponseQueryContextElement>
                {
                    new ResponseQueryContextElement(new QueryContextElement(42, 1000L, 1, "error_context"))
                }
            };
            var response = new QueryExecResponse
            {
                success = false,
                code = 500,
                message = "query failed",
                data = new QueryExecResponseData
                {
                    sqlState = "42000",
                    queryId = "test-query-id",
                    QueryContext = queryContext,
                    parameters = new List<NameValueParameter>()
                }
            };
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext());
            Assert.Empty(session.GetQueryContextRequest().Entries);
            var statement = new SFStatement(session);

            Assert.Throws<SnowflakeDbException>(() => statement.BuildResultSet(response, CancellationToken.None));

            var cachedContext = session.GetQueryContextRequest();
            Assert.NotNull(cachedContext);
            Assert.Single(cachedContext.Entries);
            Assert.Equal(42, cachedContext.Entries[0].Id);
        }

        [SFFact]
        public void TestBuildResultSetDoesNotClearQccOnFailedResponseWithoutQueryContext()
        {
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext());
            var preExistingContext = new ResponseQueryContext
            {
                Entries = new List<ResponseQueryContextElement>
                {
                    new ResponseQueryContextElement(new QueryContextElement(99, 2000L, 1, "existing"))
                }
            };
            session.UpdateQueryContextCache(preExistingContext);
            Assert.Single(session.GetQueryContextRequest().Entries);

            var response = new QueryExecResponse
            {
                success = false,
                code = 500,
                message = "query failed",
                data = new QueryExecResponseData
                {
                    sqlState = "42000",
                    queryId = "test-query-id",
                    QueryContext = null,
                    parameters = new List<NameValueParameter>()
                }
            };
            var statement = new SFStatement(session);

            Assert.Throws<SnowflakeDbException>(() => statement.BuildResultSet(response, CancellationToken.None));

            var cachedContext = session.GetQueryContextRequest();
            Assert.NotNull(cachedContext);
            Assert.Single(cachedContext.Entries);
            Assert.Equal(99, cachedContext.Entries[0].Id);
        }

        [SFFact]
        public void TestExecuteMergesQccOnFailedResponse()
        {
            var restRequester = new Mock.MockRestRequesterWithQccOnError();
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            session.Open();
            var statement = new SFStatement(session);

            Assert.Throws<SnowflakeDbException>(() => statement.Execute(StatementContext.Default with { CommandText = "select 1" }, null));

            var cachedContext = session.GetQueryContextRequest();
            Assert.NotNull(cachedContext);
            Assert.Single(cachedContext.Entries);
            Assert.Equal(42, cachedContext.Entries[0].Id);
        }

        [SFFact]
        public async Task TestExecuteAsyncMergesQccOnFailedResponse()
        {
            var restRequester = new Mock.MockRestRequesterWithQccOnError();
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(session);

            await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                await statement.ExecuteAsync(StatementContext.Default with { CommandText = "select 1" }, null, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            var cachedContext = session.GetQueryContextRequest();
            Assert.NotNull(cachedContext);
            Assert.Single(cachedContext.Entries);
            Assert.Equal(42, cachedContext.Entries[0].Id);
        }

        [SFFact]
        public void TestSessionGoneThrowsOnExecute()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);

            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.Execute(StatementContext.Default with { CommandText = "select 1" }, null));
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.True(sfSession.IsInvalidatedForPooling());
        }

        [SFFact]
        public async Task TestSessionGoneThrowsOnExecuteAsync()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);

            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                await statement.ExecuteAsync(StatementContext.Default with { CommandText = "select 1" }, null, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.True(sfSession.IsInvalidatedForPooling());
        }

        [SFFact]
        public void TestSessionGoneThrowsOnGetResultWithId()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);

            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.GetResultWithId("mockId"));
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public async Task TestSessionGoneThrowsOnGetResultWithIdAsync()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);

            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                await statement.GetResultWithIdAsync("mockId", CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public void TestSessionGoneThrowsOnGetQueryStatus()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);

            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.GetQueryStatus("mockQueryId"));
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public async Task TestSessionGoneThrowsOnGetQueryStatusAsync()
        {
            var restRequester = new Mock.MockSessionGone();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);

            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                await statement.GetQueryStatusAsync("mockQueryId", CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Equal(SFError.SESSION_GONE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public async Task TestExecuteAsyncTimeoutThrowsRequestTimeoutWithQueryId()
        {
            // arrange — mock delays 300s, timeout is 3s
            var restRequester = new Mock.MockSlowQueryRestRequester(TimeSpan.FromSeconds(300));
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "select 1", 3, null);

            // act
            var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await statement.ExecuteAsync(statementCtx, null, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            // assert
            var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
            Assert.Equal(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);
            Assert.Contains("3", thrown.Message);
        }

        [SFFact]
        public async Task TestExecuteAsyncCancellationThrowsQueryCancelled()
        {
            // arrange — mock delays 10s, cancel after 1s
            var restRequester = new Mock.MockSlowQueryRestRequester(TimeSpan.FromSeconds(10));
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            var statementCtx = new StatementContext(null, false, false, "select 1", 0, null);

            // act — no CommandTimeout (0 = infinite), cancellation via external token
            var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await statement.ExecuteAsync(statementCtx, null, cts.Token).ConfigureAwait(false)).ConfigureAwait(false);

            // assert
            var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
            Assert.Equal(SFError.QUERY_CANCELLED.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);
        }

        [SFFact]
        public async Task TestExecuteAsyncTimeoutBeforeResponseHasNullQueryId()
        {
            // arrange — server never responds, timeout is 10s
            var restRequester = new Mock.MockNeverRespondingRestRequester();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "select 1", 10, null);

            // act
            var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await statement.ExecuteAsync(statementCtx, null, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            // assert
            var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
            Assert.Equal(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);
            Assert.Null(detail.QueryId);
        }

        [SFFact]
        public void TestSyncExecuteTimeoutDuringPollingPreservesQueryId()
        {
            // arrange — mock returns "in progress" with queryId, then OCE on polling
            var restRequester = new Mock.MockTimeoutDuringPollingRestRequester();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "select 1", 30, null);

            // act
            var thrown = Assert.Throws<OperationCanceledException>(() =>
                statement.Execute(statementCtx, null));

            // assert — the OCE wraps a SnowflakeDbException with REQUEST_TIMEOUT and the queryId
            var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
            Assert.Equal(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);
            Assert.Equal(Mock.MockTimeoutDuringPollingRestRequester.MockQueryId, detail.QueryId);
            // ExecuteSqlOtherThanPutGet extracts queryId from OCE.InnerException chain
            Assert.Equal(Mock.MockTimeoutDuringPollingRestRequester.MockQueryId, statement.GetQueryId());
        }

        [SFFact]
        public async Task TestAsyncExecuteTimeoutDuringPollingPreservesQueryId()
        {
            // arrange — mock returns "in progress" with queryId, then OCE on polling
            var restRequester = new Mock.MockTimeoutDuringPollingRestRequester();
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "select 1", 30, null);

            // act
            var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await statement.ExecuteAsync(statementCtx, null, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            // assert
            var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
            Assert.Equal(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);
            Assert.Equal(Mock.MockTimeoutDuringPollingRestRequester.MockQueryId, detail.QueryId);
        }

        [SFFact]
        public void TestPutGetExecuteWrapsOceInInternalError()
        {
            // arrange — mock throws OCE with a SnowflakeDbException inner on query Post
            var restRequester = new Mock.MockPutGetOceRestRequester(throwOceOnQuery: true);
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "PUT file:///tmp/data @~", 30, null);

            // act — "PUT " prefix triggers the PUT/GET code path
            var thrown = Assert.Throws<SnowflakeDbException>(() =>
                statement.Execute(statementCtx, null));

            // assert — ExecuteSqlWithPutGet wraps OCE in INTERNAL_ERROR
            Assert.Equal(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.IsType<OperationCanceledException>(thrown.InnerException);
        }

        [SFFact]
        public void TestPutGetExecuteOcePreservesQueryIdFromInnerException()
        {
            // arrange — mock throws OCE whose InnerException is SnowflakeDbException with QueryId
            var restRequester = new Mock.MockPutGetOceRestRequester(throwOceOnQuery: true);
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "PUT file:///tmp/data @~", 30, null);

            // act
            Assert.Throws<SnowflakeDbException>(() =>
                statement.Execute(statementCtx, null));

            // assert — the queryId from the OCE's inner SnowflakeDbException is preserved
            Assert.Equal(Mock.MockPutGetOceRestRequester.MockQueryId, statement.GetQueryId());
        }

        [SFFact]
        public void TestPutGetExecuteNonOceExceptionWrapsInInternalError()
        {
            // arrange — mock returns a PutGetExecResponse with incomplete data;
            // SFFileTransferAgent will throw due to missing fields
            var restRequester = new Mock.MockPutGetOceRestRequester(throwOceOnQuery: false);
            var sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            var statement = new SFStatement(sfSession);
            var statementCtx = new StatementContext(null, false, false, "PUT file:///tmp/data @~", 0, null);

            // act — the file transfer agent will fail due to incomplete response data
            var thrown = Assert.Throws<SnowflakeDbException>(() =>
                statement.Execute(statementCtx, null));

            // assert — SFFileTransferAgent throws IO_ERROR_ON_GETPUT_COMMAND for incomplete data,
            // caught by the SnowflakeDbException catch block and re-thrown as-is
            Assert.Equal(SFError.IO_ERROR_ON_GETPUT_COMMAND.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }
    }
}
