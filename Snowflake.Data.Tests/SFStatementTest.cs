/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System;

    /**
     * Mock rest request to test session renew
     */
    [TestFixture]
    class SFStatementTest
    {
        [Test]
         public void TestSessionRenew()
        {
            Mock.MockRestSessionExpired rest = new Mock.MockRestSessionExpired();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, rest);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession, rest);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            Assert.AreEqual("new_session_token", sfSession.sessionToken);
            Assert.AreEqual("new_master_token", sfSession.masterToken);
        }

        [Test]
        public void TestSessionRenewDuringQueyrExec()
        {
            Mock.MockRestSessionExpiredInQueryExec rest = new Mock.MockRestSessionExpiredInQueryExec();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, rest);
            sfSession.Open();
            SFStatement statement = new SFStatement(sfSession, rest);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
        }
    }
}
