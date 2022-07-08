/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System.Data;
    using Snowflake.Data.Client;
    using System.Data.Common;

    /**
     * Mock rest request test
     */
    [TestFixture]
    class SFStatementTest : SFBaseTest
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

        [Test]
        public void TestCallStatement()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand command = conn.CreateCommand())
                {
                    try
                    {
                        command.CommandText = "ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS=true";
                        command.ExecuteNonQuery();
                    }
                    catch (SnowflakeDbException ex)
                    {
                        // Expected error - SqlState: 22023, VendorCode: 1006
                        Assert.AreEqual("22023", ex.SqlState);
                    }

                    command.CommandText = "create or replace procedure\n"
                        + "TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant)\n"
                        + "returns string language javascript as $$\n"
                        + "let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]});\n"
                        + "res.next();\n"
                        + "return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2;\n"
                        + "$$;";
                    command.ExecuteNonQuery();

                    command.CommandText = "call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Double;
                    p1.Value = 1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "[2,3]";
                    command.Parameters.Add(p2);

                    DbDataReader reader = command.ExecuteReader();

                    string result = "1 \"[2,3]\" [2,3]";
                    while (reader.Read())
                    {
                        Assert.AreEqual(result, reader.GetString(0));
                    }

                    command.CommandText = "drop procedure if exists TEST_SP_CALL_STMT_ENABLED(float, variant)";
                    command.ExecuteNonQuery();
                }
                conn.Close();
            }
        }
    }
}
