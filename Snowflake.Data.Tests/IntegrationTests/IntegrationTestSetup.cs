using System;
using System.Data;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class IntegrationTestFixture : IAsyncLifetime
    {
        public ValueTask InitializeAsync()
        {
            var testConfig = TestEnvironment.TestConfig;
            ModifySchema(testConfig.schema, SchemaAction.Create);
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            var testConfig = TestEnvironment.TestConfig;

            if (testConfig == null)
                return ValueTask.CompletedTask;

            ModifySchema(testConfig.schema, SchemaAction.Drop);
            return ValueTask.CompletedTask;
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

    [CollectionDefinition("Integration")]
    public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture> { }
}
