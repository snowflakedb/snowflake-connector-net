/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.IO;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [SetUpFixture]
    public class SFBaseTest
    {
        private const string connectionStringFmt = "scheme=https;host={0};port=443;" +
            "user={1};password={2};account={3};role={4};db={5};schema={6};warehouse={7}";

        protected string connectionString {get; set;}

        protected TestConfig testConfig { get; set; }

        public SFBaseTest()
        {
        }

        [OneTimeSetUp]
        public void SFTestSetup()
        {
#if NET46
            log4net.GlobalContext.Properties["framework"] = "net46";
            log4net.Config.XmlConfigurator.Configure();

#else
            log4net.GlobalContext.Properties["framework"] = "netcoreapp2.0";
            var logRepository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(logRepository, new FileInfo("App.config"));
#endif

            var reader = new StreamReader("parameters.json");
            var testConfigString = reader.ReadToEnd();
           
            Dictionary<string, TestConfig> testConfigs = JsonConvert.DeserializeObject<Dictionary<string, TestConfig>>(testConfigString);

            String cloud = Environment.GetEnvironmentVariable("snowflake_cloud_env");
            if (cloud == null)
            {
                // use AWS env if not specified
                cloud = "AZURE";
            }
            Assert.IsTrue(cloud == "AWS" || cloud == "AZURE", "{} is not supported. Specify AWS or AZURE as cloud environment", cloud);

            TestConfig testConnectionConfig;
            if (testConfigs.TryGetValue(cloud, out testConnectionConfig))
            {
                connectionString = String.Format(connectionStringFmt,
                    testConnectionConfig.host,
                    testConnectionConfig.user,
                    testConnectionConfig.password,
                    testConnectionConfig.account,
                    testConnectionConfig.role,
                    testConnectionConfig.database,
                    testConnectionConfig.schema,
                    testConnectionConfig.warehouse);
                this.testConfig = testConnectionConfig;
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

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_WAREHOUSE", NullValueHandling = NullValueHandling.Ignore)]
        internal string warehouse { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_DATABASE", NullValueHandling = NullValueHandling.Ignore)]
        internal string database { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_SCHEMA", NullValueHandling = NullValueHandling.Ignore)]
        internal string schema { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ROLE", NullValueHandling = NullValueHandling.Ignore)]
        internal string role { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_HOST", NullValueHandling = NullValueHandling.Ignore)]
        internal string host { get; set; }
    }
}
