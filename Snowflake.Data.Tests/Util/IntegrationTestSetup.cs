using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public static class TestEnvironment
{
    private static readonly CancellationTokenSource s_cts = new();
    private static volatile int s_initState = 0;

    static TestEnvironment()
    {
        s_cts.CancelAfter(TimeSpan.FromHours(1));
        s_cts.Token.Register(TerminateTestRun);

        ConnectionManagerTestsFacade.Init();
    }

    public static void Init()
    {
        if (Interlocked.Exchange(ref s_initState, 1) == 0)
            Console.Write(@"Test environment initialized.");
    }

    private static void TerminateTestRun()
    {
        Console.WriteLine(@"Terminating test run, as it's unlikely it can recover.");
        Environment.Exit(-1);
    }
}

public static class IntegrationTestEnvironment
{
    private static volatile int s_integrationTestsRunning;
    private static readonly SemaphoreSlim s_semaphoreSlim = new(1, 1);

    static IntegrationTestEnvironment()
    {
        s_integrationTestsRunning = 0;
        TestEnvironment.Init();
    }

    public static async Task StartIntegrationTest()
    {
        await s_semaphoreSlim.WaitAsync();

        try
        {
            if (s_integrationTestsRunning++ == 0)
            {
                await ModifySchemaAsync(TestConfigSingleton.TestConfig.schema, SchemaAction.Create);
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
                await ModifySchemaAsync(TestConfigSingleton.TestConfig.schema, SchemaAction.Drop);
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
            conn.ConnectionString = BuildConnectionString(TestConfigSingleton.TestConfig);
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
