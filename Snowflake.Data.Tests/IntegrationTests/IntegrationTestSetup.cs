using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public static class IntegrationTestEnvironment
    {
        private static int s_integrationTestsRunning;
        private static readonly SemaphoreSlim s_semaphoreSlim = new(1, 1);

        static IntegrationTestEnvironment()
        {
            s_integrationTestsRunning = 0;
        }

        public static async Task StartIntegrationTest()
        {
            await s_semaphoreSlim.WaitAsync();

            try
            {
                if (s_integrationTestsRunning++ == 0)
                {
                    await ModifySchemaAsync(TestEnvironment.TestConfig.schema, SchemaAction.Create);
                }
            }
            finally
            {
                s_semaphoreSlim.Release();
            }
        }

        public static async Task EndIntegrationTest()
        {
            await s_semaphoreSlim.WaitAsync();

            try
            {
                if (s_integrationTestsRunning-- == 0)
                {
                    // cleanup
                    await ModifySchemaAsync(TestEnvironment.TestConfig.schema, SchemaAction.Drop);
                }
            }
            finally
            {
                s_semaphoreSlim.Release();
            }
        }

        private enum SchemaAction
        {
            Create,
            Drop
        }

        private static async Task ModifySchemaAsync(string schemaName, SchemaAction schemaAction)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = BuildConnectionString(TestEnvironment.TestConfig);
                await ((SnowflakeDbConnection)conn).OpenAsync(CancellationToken.None);
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
