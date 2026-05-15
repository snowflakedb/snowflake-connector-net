using Xunit;
using Microsoft.Extensions.Logging;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [CollectionDefinition(nameof(Log4NetTestCollection), DisableParallelization = true)]
    public sealed class Log4NetTestCollection : ICollectionFixture<Log4NetTestCollection> { }

    [Collection(nameof(Log4NetTestCollection))]
    public sealed class Log4NetTest : LoggerTest
    {
        private const string Log4NetFileName = "test_log4net.log";
        public Log4NetTest()
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
