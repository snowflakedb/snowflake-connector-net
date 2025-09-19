using NUnit.Framework;
using Microsoft.Extensions.Logging;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture, NonParallelizable]
    class Log4NetTest : ILoggerTest
    {
        private const string Log4NetFileName = "test_log4net.log";

        [OneTimeSetUp]
        public void SetUp()
        {
            Environment.SetEnvironmentVariable("TEST_LOG4NET_FILE_NAME", Log4NetFileName);
            var factory = LoggerFactory.Create(
                builder => builder
                .AddLog4Net("TestLog4Net.config")
                .SetMinimumLevel(LogLevel.Trace));

            _customLogger = factory.CreateLogger("Log4NetTest");
            SnowflakeDbLoggerConfig.SetCustomLogger(_customLogger);
            _logFile = Log4NetFileName;
        }
    }
}
