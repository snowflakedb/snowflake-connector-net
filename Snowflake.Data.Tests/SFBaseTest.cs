/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.IO;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using NUnit.Framework.Interfaces;
    using Newtonsoft.Json;

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
     * This is the base class for all tests that call async metodes in the library - it does not use a special SynchronizationContext
     * 
     */
    [SetUpFixture]
    public class SFBaseTestAsync
    {
        private const string connectionStringWithoutAuthFmt = "scheme={0};host={1};port={2};" +
            "account={3};role={4};db={5};schema={6};warehouse={7}";

        protected string ConnectionStringWithoutAuth
        {
            get
            {
                return String.Format(connectionStringWithoutAuthFmt,
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.account,
                    testConfig.role,
                    testConfig.database,
                    testConfig.schema,
                    testConfig.warehouse);
            }
        }
        private const string connectionStringSnowflakeAuthFmt = ";user={0};password={1};";

        protected string ConnectionString
        {
            get {
                return ConnectionStringWithoutAuth +
                    String.Format(connectionStringSnowflakeAuthFmt,
                    testConfig.user,
                    testConfig.password);
            }
        }

        protected TestConfig testConfig { get; set; }

        [OneTimeSetUp]
        public void SFTestSetup()
        {
#if NETFRAMEWORK
            log4net.GlobalContext.Properties["framework"] = "net472";
            log4net.Config.XmlConfigurator.Configure();

#else
            log4net.GlobalContext.Properties["framework"] = "netcoreapp2.0";
            var logRepository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(logRepository, new FileInfo("App.config"));
#endif
            String cloud = Environment.GetEnvironmentVariable("snowflake_cloud_env");
            Assert.IsTrue(cloud == null || cloud == "AWS" || cloud == "AZURE" || cloud == "GCP", "{0} is not supported. Specify AWS, AZURE or GCP as cloud environment", cloud);

            StreamReader reader = new StreamReader("parameters.json");

            var testConfigString = reader.ReadToEnd();
           
            Dictionary<string, TestConfig> testConfigs = JsonConvert.DeserializeObject<Dictionary<string, TestConfig>>(testConfigString);

            TestConfig testConnectionConfig;
            if (testConfigs.TryGetValue("testconnection", out testConnectionConfig))
            {
                testConfig = testConnectionConfig;
            }
            else
            {
                Assert.Fail("Failed to load test configuration");
            }
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
        internal string OktaUser { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
        internal string OktaPassword { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_URL", NullValueHandling = NullValueHandling.Ignore)]
        internal string OktaURL { get; set; }

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

        public TestConfig()
        {
            this.protocol = "https";
            this.port = "443";
        }
    }

    public class IgnoreOnEnvIsAttribute : Attribute, ITestAction
    {
        String key;

        string[] values;
        public IgnoreOnEnvIsAttribute(String key, string[] values)
        {
            this.key = key;
            this.values = values;
        }

        public void BeforeTest(ITest test)
        {
            foreach (var value in this.values)
            {
                if (Environment.GetEnvironmentVariable(key) == value)
                {
                    Assert.Ignore("Test is ignored when environment variable {0} is {1} ", key, value);
                }
            }
        }

        public void AfterTest(ITest test)
        {
        }

        public ActionTargets Targets
        {
            get { return ActionTargets.Test | ActionTargets.Suite; }
        }
    }
}
