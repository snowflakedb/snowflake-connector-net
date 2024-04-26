/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;
using NUnit.Framework;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFSessionTest
    {
        // Mock test for session gone
        [Test]
        public void TestSessionGoneWhenClose()
        {
            Mock.MockCloseSessionGone restRequester = new Mock.MockCloseSessionGone();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            Assert.DoesNotThrow(() => sfSession.close());
        }

        [Test]
        public void TestUpdateSessionProperties()
        {
            // arrange
            string databaseName = "DB_TEST";
            string schemaName = "SC_TEST";
            string warehouseName = "WH_TEST";
            string roleName = "ROLE_TEST";
            QueryExecResponseData queryExecResponseData = new QueryExecResponseData
            {
                finalSchemaName = schemaName,
                finalDatabaseName = databaseName,
                finalRoleName = roleName,
                finalWarehouseName = warehouseName
            };

            // act
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null);
            sfSession.UpdateSessionProperties(queryExecResponseData);

            // assert
            Assert.AreEqual(databaseName, sfSession.database);
            Assert.AreEqual(schemaName, sfSession.schema);
            Assert.AreEqual(warehouseName, sfSession.warehouse);
            Assert.AreEqual(roleName, sfSession.role);
        }

        [Test]
        public void TestSkipUpdateSessionPropertiesWhenPropertiesMissing()
        {
            // arrange
            string databaseName = "DB_TEST";
            string schemaName = "SC_TEST";
            string warehouseName = "WH_TEST";
            string roleName = "ROLE_TEST";
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null);
            sfSession.database = databaseName;
            sfSession.warehouse = warehouseName;
            sfSession.role = roleName;
            sfSession.schema = schemaName;

            // act
            QueryExecResponseData queryExecResponseWithoutData = new QueryExecResponseData();
            sfSession.UpdateSessionProperties(queryExecResponseWithoutData);

            // assert
            // when database or schema name is missing in the response,
            // the cached value should keep unchanged
            Assert.AreEqual(databaseName, sfSession.database);
            Assert.AreEqual(schemaName, sfSession.schema);
            Assert.AreEqual(warehouseName, sfSession.warehouse);
            Assert.AreEqual(roleName, sfSession.role);
        }

        [Test]
        [TestCase(null)]
        [TestCase("/some-path/config.json")]
        [TestCase("C:\\some-path\\config.json")]
        public void TestThatConfiguresEasyLogging(string configPath)
        {
            // arrange
            var easyLoggingStarter = new Moq.Mock<EasyLoggingStarter>();
            var simpleConnectionString = "account=test;user=test;password=test;";
            var connectionString = configPath == null
                ? simpleConnectionString
                : $"{simpleConnectionString}client_config_file={configPath};";

            // act
            new SFSession(connectionString, null, easyLoggingStarter.Object);

            // assert
            easyLoggingStarter.Verify(starter => starter.Init(configPath));
        }
    }
}
