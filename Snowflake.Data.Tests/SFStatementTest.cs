﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;

    /**
     * Mock rest request test
     */
    [TestFixture]
    class SFStatementTest
    {
        [Test]
        [Ignore("StatementTest")]
        public void StatementTestDone()
        {
            // Do nothing;
        }

        // Mock test for session token renew
        [Test]
        public void TestSessionRenew()
        {
            Mock.MockRestSessionExpired restRequester = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
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
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        /// <summary>
        /// Running a query involves the SFStatement.TrimSql method.  There was a bug in the method that resulted in
        /// an unexpeced exception.  This test will ensure the condition is tested for block comments.
        /// </summary>
        [Test]
        public void TestTrimSqlBlockComment()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "/*comment*/select 1/*comment*/", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }

        /// <summary>
        /// Running a query involves the SFStatement.TrimSql method.  There was a bug in the method that resulted in
        /// an unexpeced exception.  This test will ensure the condition is tested for line comments.
        /// </summary>
        [Test]
        public void TestTrimSqlLineComment()
        {
            Mock.MockRestSessionExpiredInQueryExec restRequester = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "--comment\r\nselect 1\r\n--comment", null, false);
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
                SFBaseResultSet resultSet = statement.Execute(0, "SELECT 1", null, false);
                expectServiceName += "a";
                Assert.AreEqual(expectServiceName, sfSession.ParameterMap[SFSessionParameter.SERVICE_NAME]);
            }
        }
    }
}
