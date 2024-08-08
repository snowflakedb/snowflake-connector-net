/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

[assembly:LevelOfParallelism(10)]

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using NUnit.Framework.Interfaces;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    /*
     * This is the base class for all tests that call blocking methods in the library - it uses MockSynchronizationContext to verify that
     * there are no async deadlocks in the library
     *
     */
    [TestFixture]
    public class SFBaseTest : SFBaseTestAsync
    {
        [SetUp]
        public static void SetUpContext()
        {
            MockSynchronizationContext.SetupContext();
        }

        [TearDown]
        public static void TearDownContext()
        {
            MockSynchronizationContext.Verify();
        }
    }

    /*
     * This is the base class for all tests that call async methods in the library - it does not use a special SynchronizationContext
     *
     */
    [TestFixture]
    [FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
    [SetCulture("en-US")]
    #if !SEQUENTIAL_TEST_RUN
    [Parallelizable(ParallelScope.All)]
    #endif
    public class SFBaseTestAsync
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFBaseTestAsync>();

        private const string ConnectionStringWithoutAuthFmt = "scheme={0};host={1};port={2};" +
                                                              "account={3};role={4};db={5};schema={6};warehouse={7}";
        private const string ConnectionStringSnowflakeAuthFmt = ";user={0};password={1};";
        protected virtual string TestName => TestContext.CurrentContext.Test.MethodName;
        protected string TestNameWithWorker => TestName + TestContext.CurrentContext.WorkerId?.Replace("#", "_");
        protected string TableName => TestNameWithWorker;


        private Stopwatch _stopwatch;

        private List<string> _tablesToRemove;

        [SetUp]
        public void BeforeTest()
        {
            _stopwatch = new Stopwatch();
            _stopwatch.Start();
            _tablesToRemove = new List<string>();
        }

        [TearDown]
        public void AfterTest()
        {
            _stopwatch.Stop();
            var testName = $"{TestContext.CurrentContext.Test.FullName}";

            TestEnvironment.RecordTestPerformance(testName, _stopwatch.Elapsed);
            RemoveTables();
        }

        private void RemoveTables()
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
                    cmd.ExecuteNonQuery();
                }
            }
        }

        protected void CreateOrReplaceTable(IDbConnection conn, string tableName, IEnumerable<string> columns, string additionalQueryStr = null)
        {
            CreateOrReplaceTable(conn, tableName, "", columns, additionalQueryStr);
        }

        protected void CreateOrReplaceTable(IDbConnection conn, string tableName, string tableType, IEnumerable<string> columns, string additionalQueryStr = null)
        {
            var columnsStr = string.Join(", ", columns);
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE OR REPLACE {tableType} TABLE {tableName}({columnsStr}) {additionalQueryStr}";
            s_logger.Debug(cmd.CommandText);
            cmd.ExecuteNonQuery();

            _tablesToRemove.Add(tableName);
        }

        protected void AddTableToRemoveList(string tableName)
        {
            _tablesToRemove.Add(tableName);
        }

        public SFBaseTestAsync()
        {
            testConfig = TestEnvironment.TestConfig;
        }

        protected string ConnectionStringWithoutAuth => string.Format(ConnectionStringWithoutAuthFmt,
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.account,
                    testConfig.role,
                    testConfig.database,
                    testConfig.schema,
                    testConfig.warehouse);

        protected string ConnectionString => ConnectionStringWithoutAuth +
                                             string.Format(ConnectionStringSnowflakeAuthFmt,
                                                 testConfig.user,
                                                 testConfig.password);

        protected string ConnectionStringWithInvalidUserName => ConnectionStringWithoutAuth +
                                             string.Format(ConnectionStringSnowflakeAuthFmt,
                                                 "unknown",
                                                 testConfig.password);

        protected TestConfig testConfig { get; }

        protected string ResolveHost()
        {
            return testConfig.host ?? $"{testConfig.account}.snowflakecomputing.com";
        }
    }

    [SetUpFixture]
    public class TestEnvironment
    {
        private const string ConnectionStringFmt = "scheme={0};host={1};port={2};" +
                                                   "account={3};role={4};db={5};warehouse={6};user={7};password={8};";

        public static TestConfig TestConfig { get; private set; }

        private static Dictionary<string, TimeSpan> s_testPerformance;

        private static readonly object s_testPerformanceLock = new object();

        public static void RecordTestPerformance(string name, TimeSpan time)
        {
            lock (s_testPerformanceLock)
            {
                s_testPerformance[name] = time;
            }
        }

        [OneTimeSetUp]
        public void Setup()
        {
#if NETFRAMEWORK
            log4net.GlobalContext.Properties["framework"] = "net471";
            log4net.Config.XmlConfigurator.Configure();

#else
            log4net.GlobalContext.Properties["framework"] = "net6.0";
            var logRepository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(logRepository, new FileInfo("App.config"));
#endif
            var cloud = Environment.GetEnvironmentVariable("snowflake_cloud_env");
            Assert.IsTrue(cloud == null || cloud == "AWS" || cloud == "AZURE" || cloud == "GCP", "{0} is not supported. Specify AWS, AZURE or GCP as cloud environment", cloud);

            var reader = new StreamReader("parameters.json");

            var testConfigString = reader.ReadToEnd();

            // Local JSON settings to avoid using system wide settings which could be different
            // than the default ones
            var jsonSettings = new JsonSerializerSettings
            {
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new DefaultNamingStrategy()
                }
            };
            var testConfigs = JsonConvert.DeserializeObject<Dictionary<string, TestConfig>>(testConfigString, jsonSettings);

            if (testConfigs.TryGetValue("testconnection", out var testConnectionConfig))
            {
                TestConfig = testConnectionConfig;
                TestConfig.schema = TestConfig.schema + "_" + Guid.NewGuid().ToString().Replace("-", "_");
            }
            else
            {
                Assert.Fail("Failed to load test configuration");
            }

            ModifySchema(TestConfig.schema, SchemaAction.CREATE);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            ModifySchema(TestConfig.schema, SchemaAction.DROP);
        }

        [OneTimeSetUp]
        public void SetupTestPerformance()
        {
            s_testPerformance = new Dictionary<string, TimeSpan>();
        }

        [OneTimeTearDown]
        public void CreateTestTimeArtifact()
        {
            var resultText = "test;time_in_ms\n";
            resultText += string.Join("\n",
                s_testPerformance.Select(test => $"{test.Key};{Math.Round(test.Value.TotalMilliseconds,0)}"));

            var dotnetVersion = Environment.GetEnvironmentVariable("net_version");
            var cloudEnv = Environment.GetEnvironmentVariable("snowflake_cloud_env");

            var separator = Path.DirectorySeparatorChar;

            // We have to go up 3 times as the working directory path looks as follows:
            // Snowflake.Data.Tests/bin/debug/{.net_version}/
            File.WriteAllText($"..{separator}..{separator}..{separator}{GetOs()}_{dotnetVersion}_{cloudEnv}_performance.csv", resultText);
        }

        private static string s_connectionString => string.Format(ConnectionStringFmt,
            TestConfig.protocol,
            TestConfig.host,
            TestConfig.port,
            TestConfig.account,
            TestConfig.role,
            TestConfig.database,
            TestConfig.warehouse,
            TestConfig.user,
            TestConfig.password);

        private enum SchemaAction
        {
            CREATE,
            DROP
        }

        private static void ModifySchema(string schemaName, SchemaAction schemaAction)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = s_connectionString;
                conn.Open();
                var dbCommand = conn.CreateCommand();
                switch (schemaAction)
                {
                    case SchemaAction.CREATE:
                        dbCommand.CommandText = $"CREATE OR REPLACE SCHEMA {schemaName}";
                        break;
                    case SchemaAction.DROP:
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

        private static string GetOs()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return "windows";
            }
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return "linux";
            }
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return "macos";
            }

            return "unknown";
        }
    }

    public class TestConfig
    {
        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_USER", NullValueHandling = NullValueHandling.Ignore)]
        internal string user { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
        internal string password { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ACCOUNT", NullValueHandling = NullValueHandling.Ignore)]
        internal string account { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_HOST", NullValueHandling = NullValueHandling.Ignore)]
        internal string host { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PORT", NullValueHandling = NullValueHandling.Ignore)]
        internal string port { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_WAREHOUSE", NullValueHandling = NullValueHandling.Ignore)]
        internal string warehouse { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_DATABASE", NullValueHandling = NullValueHandling.Ignore)]
        internal string database { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_SCHEMA", NullValueHandling = NullValueHandling.Ignore)]
        internal string schema { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ROLE", NullValueHandling = NullValueHandling.Ignore)]
        internal string role { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PROTOCOL", NullValueHandling = NullValueHandling.Ignore)]
        internal string protocol { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_USER", NullValueHandling = NullValueHandling.Ignore)]
        internal string oktaUser { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
        internal string oktaPassword { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_URL", NullValueHandling = NullValueHandling.Ignore)]
        internal string oktaUrl { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_JWT_USER", NullValueHandling = NullValueHandling.Ignore)]
        internal string jwtAuthUser { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PEM_FILE", NullValueHandling = NullValueHandling.Ignore)]
        internal string pemFilePath { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_P8_FILE", NullValueHandling = NullValueHandling.Ignore)]
        internal string p8FilePath { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PWD_PROTECTED_PK_FILE", NullValueHandling = NullValueHandling.Ignore)]
        internal string pwdProtectedPrivateKeyFilePath { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PK_CONTENT", NullValueHandling = NullValueHandling.Ignore)]
        internal string privateKey { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PROTECTED_PK_CONTENT", NullValueHandling = NullValueHandling.Ignore)]
        internal string pwdProtectedPrivateKey { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PK_PWD", NullValueHandling = NullValueHandling.Ignore)]
        internal string privateKeyFilePwd { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_TOKEN", NullValueHandling = NullValueHandling.Ignore)]
        internal string oauthToken { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_EXP_OAUTH_TOKEN", NullValueHandling = NullValueHandling.Ignore)]
        internal string expOauthToken { get; set; }

        [JsonProperty(PropertyName = "PROXY_HOST", NullValueHandling = NullValueHandling.Ignore)]
        internal string proxyHost { get; set; }

        [JsonProperty(PropertyName = "PROXY_PORT", NullValueHandling = NullValueHandling.Ignore)]
        internal string proxyPort { get; set; }

        [JsonProperty(PropertyName = "AUTH_PROXY_HOST", NullValueHandling = NullValueHandling.Ignore)]
        internal string authProxyHost { get; set; }

        [JsonProperty(PropertyName = "AUTH_PROXY_PORT", NullValueHandling = NullValueHandling.Ignore)]
        internal string authProxyPort { get; set; }

        [JsonProperty(PropertyName = "AUTH_PROXY_USER", NullValueHandling = NullValueHandling.Ignore)]
        internal string authProxyUser { get; set; }

        [JsonProperty(PropertyName = "AUTH_PROXY_PWD", NullValueHandling = NullValueHandling.Ignore)]
        internal string authProxyPwd { get; set; }

        [JsonProperty(PropertyName = "NON_PROXY_HOSTS", NullValueHandling = NullValueHandling.Ignore)]
        internal string nonProxyHosts { get; set; }

        public TestConfig()
        {
            protocol = "https";
            port = "443";
        }
    }

    public class IgnoreOnEnvIsAttribute : Attribute, ITestAction
    {
        private readonly string _key;

        private readonly string[] _values;

        private readonly string _reason;

        public IgnoreOnEnvIsAttribute(string key, string[] values, string reason = null)
        {
            _key = key;
            _values = values;
            _reason = reason;
        }

        public void BeforeTest(ITest test)
        {
            foreach (var value in _values)
            {
                if (Environment.GetEnvironmentVariable(_key) == value)
                {
                    Assert.Ignore("Test is ignored when environment variable {0} is {1}. {2}", _key, value, _reason);
                }
            }
        }

        public void AfterTest(ITest test)
        {
        }

        public ActionTargets Targets => ActionTargets.Test | ActionTargets.Suite;
    }

    public class IgnoreOnCI : IgnoreOnEnvIsAttribute
    {
        public IgnoreOnCI(string reason = null) : base("CI", new[] { "true" }, reason)
        {
        }
    }
}
