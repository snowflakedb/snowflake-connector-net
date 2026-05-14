using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
#if NET8_0_OR_GREATER
using TaskOrValueTask = System.Threading.Tasks.ValueTask;
#else
using TaskOrValueTask = System.Threading.Tasks.Task;
#endif

namespace Snowflake.Data.Tests.IntegrationTests
{
    public static class IntegrationTestEnvironment
    {
        private static volatile int s_integrationTestsTotal;
        private static volatile int s_integrationTestsRun;

        static IntegrationTestEnvironment()
        {
            s_integrationTestsRun = 0;
            s_integrationTestsTotal = Assembly.GetExecutingAssembly()
                .GetTypes()
                .Where(x => x.IsAssignableTo(typeof(SFBaseTestAsync)))
                .Count(x => !x.IsAbstract);

            ModifySchemaAsync(TestEnvironment.TestConfig.schema, SchemaAction.Create).GetAwaiter().GetResult();
        }

        public static void IntegrationTestRun()
        {
            Interlocked.Increment(ref s_integrationTestsRun);

            if (s_integrationTestsRun == s_integrationTestsTotal)
            {
                // cleanup
                ModifySchemaAsync(TestEnvironment.TestConfig.schema, SchemaAction.Drop).GetAwaiter().GetResult();
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
