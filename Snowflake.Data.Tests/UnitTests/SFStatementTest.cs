﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System;

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
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            Assert.AreEqual("new_session_token", sfSession.sessionToken);
            Assert.AreEqual("new_master_token", sfSession.masterToken);
            Assert.AreEqual(restRequester.FirstTimeRequestID, restRequester.SecondTimeRequestID);
        }

        // Mock test for session renew during query execution
        [Test]
        public void TestSessionRenewDuringQueryExec()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
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
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
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
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "/*comment*/select 1/*comment*/", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a multiline block comment
        /// </summary>
        [Test]
        public void TestTrimSqlBlockCommentMultiline()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "/*comment\r\ncomment*/select 1/*comment\r\ncomment*/", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment
        /// </summary>
        [Test]
        public void TestTrimSqlLineComment()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "--comment\r\nselect 1\r\n--comment", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        /// <summary>
        /// Ensure TrimSql stops reading the query when no more characters after a line comment with a closing newline
        /// </summary>
        [Test]
        public void TestTrimSqlLineCommentWithClosingNewline()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "--comment\r\nselect 1\r\n--comment\r\n", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
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
            Assert.AreEqual(expectedResult, QueryStatuses.IsStillRunning(status));
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
            Assert.AreEqual(expectedResult, QueryStatuses.IsAnError(status));
        }
    }
}
