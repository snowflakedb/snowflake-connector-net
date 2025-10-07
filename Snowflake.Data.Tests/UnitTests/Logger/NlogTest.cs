using NUnit.Framework;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture, NonParallelizable]
    class NlogTest : ILoggerTest
    {
        private const string NlogFileName = "test_nlog.log";

        [OneTimeSetUp]
        public void SetUp()
        {
            Environment.SetEnvironmentVariable("TEST_NLOG_FILE_NAME", NlogFileName);
            var factory = LoggerFactory.Create(
                builder => builder
                .AddNLog("TestNLog.config")
                .SetMinimumLevel(LogLevel.Trace));

            _customLogger = factory.CreateLogger("NlogTest");
            SnowflakeDbLoggerConfig.SetCustomLogger(_customLogger);
            _logFile = NlogFileName;
        }
    }
}
