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

            // for now hardcode to get "testconnection"
            TestConfig testConnectionConfig;
            if (testConfigs.TryGetValue("testconnection", out testConnectionConfig))
            {
                // Add host to connection string only if it is specified.
                var host = testConnectionConfig.host ?? string.Empty;
                if (host != string.Empty)
                {
                    host = $"host={host};";
                }

                connectionString = $"scheme=https;{host}port=443;" +
                                   $"user={testConnectionConfig.user};password={testConnectionConfig.password};" +
                                   $"account={testConnectionConfig.account};role={testConnectionConfig.role};" +
                                   $"db={testConnectionConfig.database};schema={testConnectionConfig.schema};warehouse={testConnectionConfig.warehouse}";
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

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_WAREHOUSE", NullValueHandling = NullValueHandling.Ignore)]
        internal string warehouse { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_DATABASE", NullValueHandling = NullValueHandling.Ignore)]
        internal string database { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_SCHEMA", NullValueHandling = NullValueHandling.Ignore)]
        internal string schema { get; set; }

        [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ROLE", NullValueHandling = NullValueHandling.Ignore)]
        internal string role { get; set; }
    }
}
