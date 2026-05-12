using System;
using System.Data;
using Xunit;
using Snowflake.Data.Client;
#if NET8_0_OR_GREATER
using TaskOrValueTask = System.Threading.Tasks.ValueTask;
#else
using TaskOrValueTask = System.Threading.Tasks.Task;
#endif

namespace Snowflake.Data.Tests.IntegrationTests
{
    [CollectionDefinition(IntegrationTestCollectionName)]
    public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
    {
        public const string IntegrationTestCollectionName = "IntegrationTest";
    }

    public class IntegrationTestFixture : TestEnvironmentFixture, IAsyncLifetime
    {
        public TaskOrValueTask InitializeAsync()
        {
            ModifySchema(TestConfig.schema, SchemaAction.Create);
            return TaskOrValueTask.CompletedTask;
        }

        public TaskOrValueTask DisposeAsync()
        {
            if (TestConfig == null)
                return TaskOrValueTask.CompletedTask;

            ModifySchema(TestConfig.schema, SchemaAction.Drop);
            Dispose();
            return TaskOrValueTask.CompletedTask;
        }

        private enum SchemaAction
        {
            Create,
            Drop
        }

        private void ModifySchema(string schemaName, SchemaAction schemaAction)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = BuildConnectionString(TestConfig);
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
