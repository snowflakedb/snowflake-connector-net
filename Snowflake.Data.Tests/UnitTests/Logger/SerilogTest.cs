using System;
using Xunit;
using Serilog;
using Serilog.Extensions.Logging;
using Snowflake.Data.Client;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    // TODO handle these global state thingies
    public sealed class SerilogTestFixture : IDisposable
    {
        private const string SerilogFileName = "test_serilog.log";

        public readonly ILogger _customLogger;
        private readonly object _logFile;

        public SerilogTestFixture()
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

        public void Dispose()
        {
            // TODO release managed resources here
        }
    }

    public class SerilogTest : LoggerTest, IClassFixture<SerilogTestFixture>
    {
        public SerilogTest(SerilogTestFixture fixture)
        {
            _customLogger = fixture._customLogger;
            _logFile = "test_serilog.log";
        }
    }
}
