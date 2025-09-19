using NUnit.Framework;
using Serilog;
using Serilog.Extensions.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture, NonParallelizable]
    class SerilogTest : ILoggerTest
    {
        private const string SerilogFileName = "test_serilog.log";

        [OneTimeSetUp]
        public void SetUp()
        {
            var loggerSerilog = new LoggerConfiguration()
                //.ReadFrom.Xml("TestSerilog.Config")
                .MinimumLevel.Verbose()
                .WriteTo.File(SerilogFileName)
                .CreateLogger();

            _customLogger = new SerilogLoggerFactory(loggerSerilog).CreateLogger("SerilogTest");
            SnowflakeDbLoggerConfig.SetCustomLogger(_customLogger);
            _logFile = SerilogFileName;
        }
    }
}
