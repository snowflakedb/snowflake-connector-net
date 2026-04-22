using System;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [SetUpFixture]
    public class IntegrationTestSetup
    {
        internal static TestContextAppender TestContextAppender { get; private set; }

        [OneTimeSetUp]
        public void SetupIntegrationTests()
        {
            TestContextAppender = new TestContextAppender
            {
                PatternLayout = new PatternLayout
                {
                    ConversionPattern = "[%date] [%t] [%-5level] [%logger] %message%newline"
                }
            };
            TestContextAppender.ActivateOptions();
            SFLoggerImpl.s_appenders.Add(TestContextAppender);
            SFLoggerImpl.SetLevel(LoggingEvent.DEBUG);

            var testConfig = TestEnvironment.TestConfig;
            ModifySchema(testConfig.schema, SchemaAction.Create);
        }

        [OneTimeTearDown]
        public void CleanupIntegrationTests()
        {
            if (TestContextAppender != null)
            {
                SFLoggerImpl.s_appenders.Remove(TestContextAppender);
                TestContextAppender = null;
            }

            var testConfig = TestEnvironment.TestConfig;

            if (testConfig == null)
                return;

            ModifySchema(testConfig.schema, SchemaAction.Drop);
        }

        private enum SchemaAction
        {
            Create,
            Drop
        }

        private static void ModifySchema(string schemaName, SchemaAction schemaAction)
        {
            var testConfig = TestEnvironment.TestConfig;

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = BuildConnectionString(testConfig);
                conn.Open();
                var dbCommand = conn.CreateCommand();

                switch (schemaAction)
                {
                    case SchemaAction.Create:
                        dbCommand.CommandText = $"CREATE OR REPLACE SCHEMA {schemaName}";
                        break;
                    case SchemaAction.Drop:
                        dbCommand.CommandText = $"DROP SCHEMA IF EXISTS {schemaName}";
                        break;
                    default:
                        Assert.Fail($"Not supported action on schema: {schemaAction}");
                        break;
                }

                try
                {
                    dbCommand.ExecuteNonQuery();
                }
                catch (InvalidOperationException e)
                {
                    Assert.Fail($"Unable to {schemaAction.ToString().ToLower()} schema {schemaName}:\n{e.StackTrace}");
                }
            }
        }

        private static string BuildConnectionString(TestConfig config)
        {
            return $"scheme={config.protocol};host={config.host};port={config.port};" +
                   $"certRevocationCheckMode=enabled;account={config.account};role={config.role};" +
                   $"db={config.database};warehouse={config.warehouse};" +
                   $"user={config.user};password={config.password};";
        }
    }
}

