using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.IntegrationTests;
using Xunit;
#if NET8_0_OR_GREATER
using TaskOrValueTask = System.Threading.Tasks.ValueTask;
#else
using TaskOrValueTask = System.Threading.Tasks.Task;
#endif

[assembly: CollectionBehavior(MaxParallelThreads = 20)]

namespace Snowflake.Data.Tests
{
    // todo tests all ITs call start end?
    // TODO pass around cancellationtoken
    public abstract class SFBaseTestAsync : IClassFixture<SFBaseTestAsyncFixture>
    {
        protected CancellationToken CancellationToken;

        protected SFBaseTestAsync(SFBaseTestAsyncFixture fixture)
        {
            #if NET8_0_OR_GREATER
            CancellationToken = TestContext.Current.CancellationToken;
#else
            CancellationToken = CancellationToken.None;
#endif
        }
    }

    public class SFBaseTestAsyncFixture : IAsyncLifetime
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFBaseTestAsyncFixture>();

        private const string ConnectionStringWithoutAuthFmt = "scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
                                                              "account={3};role={4};db={5};schema={6};warehouse={7};";

        private const string ConnectionStringSnowflakeAuthFmt = ";user={0};password={1};";
        private const string ConnectionStringJwtAuthFmt = ";authenticator=snowflake_jwt;user={0};private_key_file={1};";
        private const string ConnectionStringJwtContentFmt = ";authenticator=snowflake_jwt;user={0};private_key={1};";

        public string TestName => $"{GetType().Name}";
        public string TableName => TestName; // todo naming

        private readonly Stopwatch _stopwatch;
        private readonly ConcurrentStack<string> _tablesToRemove;
        private bool _anyTestStarted;

        public SFBaseTestAsyncFixture()
        {
            _stopwatch = new Stopwatch();
            _stopwatch.Start();
            _tablesToRemove = new ConcurrentStack<string>();
        }

        public virtual async TaskOrValueTask InitializeAsync()
        {
            await IntegrationTestEnvironment.StartIntegrationTest();
            _anyTestStarted = true;
            testConfig = TestConfigSingleton.TestConfig;
        }

        public virtual async TaskOrValueTask DisposeAsync()
        {
            if (_anyTestStarted)
            {
                _stopwatch.Stop();
                var testName = GetType().FullName + "." + TestName;
                // TODO
                //_envFixture.RecordTestPerformance(testName, _stopwatch.Elapsed);
            }
            await RemoveTables();
            await IntegrationTestEnvironment.EndIntegrationTest();
        }

        private async Task RemoveTables()
        {
            if (_tablesToRemove.Count == 0)
                return;

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();

                foreach (var table in _tablesToRemove)
                {
                    cmd.CommandText = $"DROP TABLE IF EXISTS {table}";
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public void CreateOrReplaceTable(IDbConnection conn, string tableName, IEnumerable<string> columns, string additionalQueryStr = null)
        {
            CreateOrReplaceTable(conn, tableName, "", columns, additionalQueryStr);
        }

        public void CreateOrReplaceTable(IDbConnection conn, string tableName, string tableType, IEnumerable<string> columns,
            string additionalQueryStr = null)
        {
            var columnsStr = string.Join(", ", columns);
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE OR REPLACE {tableType} TABLE {tableName}({columnsStr}) {additionalQueryStr}";
            s_logger.Debug(cmd.CommandText);
            cmd.ExecuteNonQuery();

            _tablesToRemove.Push(tableName);
        }

        public void AddTableToRemoveList(string tableName)
        {
            _tablesToRemove.Push(tableName);
        }

        public string ConnectionStringWithoutAuth => string.Format(ConnectionStringWithoutAuthFmt,
            testConfig.protocol,
            testConfig.host,
            testConfig.port,
            testConfig.account,
            testConfig.role,
            testConfig.database,
            testConfig.schema,
            testConfig.warehouse);

        public string ConnectionString => ConnectionStringWithoutAuth + GetAuthenticationString();

        private string GetAuthenticationString()
        {
            // 1. Jenkins override - always use password authentication
            if (IsRunningInJenkins())
            {
                return string.Format(ConnectionStringSnowflakeAuthFmt,
                    testConfig.user,
                    testConfig.password);
            }

            // 2. Try RSA key file path (discovered file)
            var keyFilePath = DiscoverRsaKeyFile();
            if (!string.IsNullOrEmpty(keyFilePath))
            {
                return string.Format(ConnectionStringJwtAuthFmt,
                    testConfig.user,
                    keyFilePath);
            }

            // 3. Try RSA key content (from parameters)
            if (!string.IsNullOrEmpty(testConfig.privateKey))
            {
                return string.Format(ConnectionStringJwtContentFmt,
                    testConfig.user,
                    testConfig.privateKey);
            }

            // 4. Explicit authenticator override (for non-JWT auth like externalbrowser, etc.)
            if (!string.IsNullOrEmpty(testConfig.authenticator))
            {
                return $";authenticator={testConfig.authenticator};user={testConfig.user};password={testConfig.password};";
            }

            // 5. Fallback to password authentication
            return string.Format(ConnectionStringSnowflakeAuthFmt,
                testConfig.user,
                testConfig.password);
        }

        private string DiscoverRsaKeyFile()
        {
            // Search locations in priority order - start with CI/CD location first
            string[] searchPaths =
            {
                "../../..", // From bin/Debug/netX.0 back to Snowflake.Data.Tests (CI/CD)
                ".", // Current directory (local dev)
                "../../../..", // From bin/Debug/netX.0/publish back to Snowflake.Data.Tests
                "../../../../.." // From deeper nested directories
            };

            foreach (var searchPath in searchPaths)
            {
                if (Directory.Exists(searchPath))
                {
                    var keyFiles = Directory.GetFiles(searchPath, "rsa_key_dotnet_*.p8");
                    if (keyFiles.Length > 0)
                    {
                        var fileName = Path.GetFileName(keyFiles[0]);

                        // For current directory, just return filename
                        if (searchPath == ".")
                        {
                            return fileName;
                        }

                        // For other paths, use consistent relative path that works cross-platform
                        // Use Path.Combine but normalize to forward slashes for consistency
                        var relativePath = Path.Combine(searchPath, fileName);
                        return relativePath.Replace(Path.DirectorySeparatorChar, '/');
                    }
                }
            }

            return null;
        }

        private bool IsRunningInJenkins()
        {
            // Jenkins typically sets these environment variables
            return !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_URL")) ||
                   !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("BUILD_NUMBER")) ||
                   !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JOB_NAME"));
        }


        public string ConnectionStringWithInvalidUserName => ConnectionStringWithoutAuth +
                                                             string.Format(ConnectionStringSnowflakeAuthFmt,
                                                                 "unknown",
                                                                 testConfig.password);

        public TestConfig testConfig { get; private set; }

        public string ResolveHost()
        {
            return testConfig.host ?? $"{testConfig.account}.snowflakecomputing.com";
        }
    }
}
