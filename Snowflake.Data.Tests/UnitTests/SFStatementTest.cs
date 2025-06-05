using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using NUnit.Framework;
using System;
using System.Threading.Tasks;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    /**
     * Mock rest request test
     */
    [TestFixture]
    class SFStatementTest
    {
        // Mock test for session token renew
        [Test]
        public void TestSessionRenew()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            Assert.AreEqual("new_session_token", sfSession.sessionToken);
            Assert.AreEqual("new_master_token", sfSession.masterToken);
            Assert.AreEqual(restRequester.FirstTimeRequestID, restRequester.SecondTimeRequestID);
        }

        [Test]
        public void TestSessionRenewGetResultWithId()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.GetResultWithId("mockId");
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("abc", resultSet.GetString(0));
            Assert.AreEqual("new_session_token", sfSession.sessionToken);
            Assert.AreEqual("new_master_token", sfSession.masterToken);
        }

        [Test]
        public void TestSessionRenewGetResultWithIdOnlyRetries3Times()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            var thrown = Assert.Throws<SnowflakeDbException>(() => statement.GetResultWithId("retryId"));
            Assert.AreEqual(thrown.ErrorCode, Mock.MockRestSessionExpired.SESSION_EXPIRED_CODE);
        }

        [Test]
        public async Task TestSessionRenewGetResultWithIdAsync()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None);
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = await statement.GetResultWithIdAsync("mockId", CancellationToken.None);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("abc", resultSet.GetString(0));
            Assert.AreEqual("new_session_token", sfSession.sessionToken);
            Assert.AreEqual("new_master_token", sfSession.masterToken);
        }

        [Test]
        public async Task TestSessionRenewGetResultWithIdOnlyRetries3TimesAsync()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None);
            SFStatement statement = new SFStatement(sfSession);
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(async () => await statement.GetResultWithIdAsync("retryId", CancellationToken.None));
            Assert.AreEqual(thrown.ErrorCode, Mock.MockRestSessionExpired.SESSION_EXPIRED_CODE);
        }

        // Mock test for session renew during query execution
        [Test]
        public void TestSessionRenewDuringQueryExec()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        // Mock test for Service Name
        // The Mock requester would take in the X-Snowflake-Service header in the request
        // and append a character 'a' at the end, send back as SERVICE_NAME parameter
        // This test is to assure that SETVICE_NAME parameter would be upgraded to the session
        [Test]
        public void TestServiceName()
        {
            var restRequester = new Mock.MockServiceName();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            sfSession.Open();
            string expectServiceName = Mock.MockServiceName.INIT_SERVICE_NAME;
            Assert.AreEqual(expectServiceName, sfSession.ParameterMap[SFSessionParameter.SERVICE_NAME]);
            for (int i = 0; i < 5; i++)
            {
                SFStatement statement = new SFStatement(sfSession);
                SFBaseResultSet resultSet = statement.Execute(0, "SELECT 1", null, false, false);
                expectServiceName += "a";
                Assert.AreEqual(expectServiceName, sfSession.ParameterMap[SFSessionParameter.SERVICE_NAME]);
            }
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a block comment
        /// </summary>
        [Test]
        public void TestTrimSqlBlockComment()
        {
            const string SqlSource = "/*comment*/select 1/*comment*/";
            const string SqlExpected = "select 1";

            Assert.AreEqual(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a multiline block comment
        /// </summary>
        [Test]
        public void TestTrimSqlBlockCommentMultiline()
        {
            const string SqlSource = "/*comment\r\ncomment*/select 1/*comment\r\ncomment*/";
            const string SqlExpected = "select 1";

            Assert.AreEqual(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment
        /// </summary>
        [Test]
        public void TestTrimSqlLineComment()
        {
            const string SqlSource = "--comment\r\nselect 1\r\n--comment";
            const string SqlExpected = "select 1\r\n--comment";

            Assert.AreEqual(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment with a closing newline
        /// </summary>
        [Test]
        public void TestTrimSqlLineCommentWithClosingNewline()
        {
            const string SqlSource = "--comment\r\nselect 1\r\n--comment\r\n";
            const string SqlExpected = "select 1";

            Assert.AreEqual(SqlExpected, SFStatement.TrimSql(SqlSource));
        }

        [Test]
        [TestCase("running", QueryStatus.Running)]
        [TestCase("RUNNING", QueryStatus.Running)]
        [TestCase("resuming_warehouse", QueryStatus.ResumingWarehouse)]
        [TestCase("RESUMING_WAREHOUSE", QueryStatus.ResumingWarehouse)]
        [TestCase("queued", QueryStatus.Queued)]
        [TestCase("QUEUED", QueryStatus.Queued)]
        [TestCase("queued_reparing_warehouse", QueryStatus.QueuedReparingWarehouse)]
        [TestCase("QUEUED_REPARING_WAREHOUSE", QueryStatus.QueuedReparingWarehouse)]
        [TestCase("no_data", QueryStatus.NoData)]
        [TestCase("NO_DATA", QueryStatus.NoData)]
        [TestCase("aborting", QueryStatus.Aborting)]
        [TestCase("ABORTING", QueryStatus.Aborting)]
        [TestCase("success", QueryStatus.Success)]
        [TestCase("SUCCESS", QueryStatus.Success)]
        [TestCase("failed_with_error", QueryStatus.FailedWithError)]
        [TestCase("FAILED_WITH_ERROR", QueryStatus.FailedWithError)]
        [TestCase("aborted", QueryStatus.Aborted)]
        [TestCase("ABORTED", QueryStatus.Aborted)]
        [TestCase("failed_with_incident", QueryStatus.FailedWithIncident)]
        [TestCase("FAILED_WITH_INCIDENT", QueryStatus.FailedWithIncident)]
        [TestCase("disconnected", QueryStatus.Disconnected)]
        [TestCase("DISCONNECTED", QueryStatus.Disconnected)]
        [TestCase("restarted", QueryStatus.Restarted)]
        [TestCase("RESTARTED", QueryStatus.Restarted)]
        [TestCase("blocked", QueryStatus.Blocked)]
        [TestCase("BLOCKED", QueryStatus.Blocked)]
        public void TestGetQueryStatusByStringValue(string stringValue, QueryStatus expectedStatus)
        {
            Assert.AreEqual(expectedStatus, QueryStatusExtensions.GetQueryStatusByStringValue(stringValue));
        }

        [Test]
        [TestCase("UNKNOWN")]
        [TestCase("RANDOM_STATUS")]
        [TestCase("aBcZyX")]
        public void TestGetQueryStatusByStringValueThrowsErrorForUnknownStatus(string stringValue)
        {
            var thrown = Assert.Throws<Exception>(() => QueryStatusExtensions.GetQueryStatusByStringValue(stringValue));
            Assert.IsTrue(thrown.Message.Contains("The query status returned by the server is not recognized"));
        }

        [Test]
        [TestCase(QueryStatus.Running, true)]
        [TestCase(QueryStatus.ResumingWarehouse, true)]
        [TestCase(QueryStatus.Queued, true)]
        [TestCase(QueryStatus.QueuedReparingWarehouse, true)]
        [TestCase(QueryStatus.NoData, true)]
        [TestCase(QueryStatus.Aborting, false)]
        [TestCase(QueryStatus.Success, false)]
        [TestCase(QueryStatus.FailedWithError, false)]
        [TestCase(QueryStatus.Aborted, false)]
        [TestCase(QueryStatus.FailedWithIncident, false)]
        [TestCase(QueryStatus.Disconnected, false)]
        [TestCase(QueryStatus.Restarted, false)]
        [TestCase(QueryStatus.Blocked, false)]
        public void TestIsStillRunning(QueryStatus status, bool expectedResult)
        {
            Assert.AreEqual(expectedResult, QueryStatusExtensions.IsStillRunning(status));
        }

        [Test]
        [TestCase(QueryStatus.Aborting, true)]
        [TestCase(QueryStatus.FailedWithError, true)]
        [TestCase(QueryStatus.Aborted, true)]
        [TestCase(QueryStatus.FailedWithIncident, true)]
        [TestCase(QueryStatus.Disconnected, true)]
        [TestCase(QueryStatus.Blocked, true)]
        [TestCase(QueryStatus.Running, false)]
        [TestCase(QueryStatus.ResumingWarehouse, false)]
        [TestCase(QueryStatus.Queued, false)]
        [TestCase(QueryStatus.QueuedReparingWarehouse, false)]
        [TestCase(QueryStatus.NoData, false)]
        [TestCase(QueryStatus.Success, false)]
        [TestCase(QueryStatus.Restarted, false)]
        public void TestIsAnError(QueryStatus status, bool expectedResult)
        {
            Assert.AreEqual(expectedResult, QueryStatusExtensions.IsAnError(status));
        }

        [Test]
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
            Assert.AreEqual("Error: internal error SqlState: , VendorCode: 500, QueryId: ", thrown.Message);
        }
    }
}
