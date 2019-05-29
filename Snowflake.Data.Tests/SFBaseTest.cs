﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
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
    using NUnit.Framework.Interfaces;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [SetUpFixture]
    public class SFBaseTest
    {
        private const string connectionStringFmt = "scheme={0};host={1};port={2};" +
            "user={3};password={4};account={5};role={6};db={7};schema={8};warehouse={9}";

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
            Assert.IsTrue(cloud == null || cloud == "AWS" || cloud == "AZURE", "{0} is not supported. Specify AWS or AZURE as cloud environment", cloud);

            // get key of connection json. Default to "testconnection". If snowflake_cloud_env is specified, use that value as key to
            // find connection object
            String connectionKey = cloud == null ? "testconnection" : cloud;

            TestConfig testConnectionConfig;
            if (testConfigs.TryGetValue(connectionKey, out testConnectionConfig))
            {
                connectionString = String.Format(connectionStringFmt,
                    testConnectionConfig.protocol,
                    testConnectionConfig.host,
                    testConnectionConfig.port,
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

        public TestConfig()
        {
            this.protocol = "https";
            this.port = "443";
        }
    }

    public class IgnoreOnEnvIsAttribute : Attribute, ITestAction
    {
        String key;

        String value;
         public IgnoreOnEnvIsAttribute(String key, String value)
         {
             this.key = key;
             this.value = value;
         }

         public void BeforeTest(ITest test)
         {
             if (Environment.GetEnvironmentVariable(key) == value)
             {
                 Assert.Ignore("Test is ignored when environment variable {0} is {1} ", key, value);
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
