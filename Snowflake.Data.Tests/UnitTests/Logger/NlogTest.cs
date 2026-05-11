using Xunit;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    class NlogTest : LoggerTest
    {
        private const string NlogFileName = "test_nlog.log";
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
