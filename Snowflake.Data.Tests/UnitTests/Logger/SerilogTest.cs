using System;
using Xunit;
using Serilog;
using Serilog.Extensions.Logging;
using Snowflake.Data.Client;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [CollectionDefinition(nameof(SerilogTestCollection), DisableParallelization = true)]
    public sealed class SerilogTestCollection : ICollectionFixture<SerilogTestFixture> { }

    public sealed class SerilogTestFixture : IDisposable
    {
        private const string SerilogFileName = "test_serilog.log";

        public readonly ILogger _customLogger;

        public SerilogTestFixture()
        {
            var loggerSerilog = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.File(SerilogFileName, flushToDiskInterval: TimeSpan.Zero)
                .CreateLogger();

            _customLogger = new SerilogLoggerFactory(loggerSerilog).CreateLogger("SerilogTest");
        }

        public void Dispose()
        {
            Serilog.Log.CloseAndFlush();
        }
    }

    [Collection(nameof(SerilogTestCollection))]
    public sealed class SerilogTest : LoggerTest, IClassFixture<SerilogTestFixture>
    {
        public SerilogTest(SerilogTestFixture fixture)
        {
            _customLogger = fixture._customLogger;
            _logFile = "test_serilog.log";
        }
    }
}
