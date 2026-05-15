using System;
using Xunit;
using Serilog;
using Serilog.Extensions.Logging;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [CollectionDefinition(nameof(SerilogTestCollection), DisableParallelization = true)]
    public sealed class SerilogTestCollection : ICollectionFixture<SerilogTestFixture> { }

    public sealed class SerilogTestFixture
    { }

    [Collection(nameof(SerilogTestCollection))]
    public sealed class SerilogTest : LoggerTest, IClassFixture<SerilogTestFixture>
    {
        private const string SerilogFileName = "test_serilog.log";

        public SerilogTest()
        {
            _logFile = "test_serilog.log";

            var loggerSerilog = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.File(SerilogFileName, flushToDiskInterval: TimeSpan.Zero)
                .CreateLogger();

            _customLogger = new SerilogLoggerFactory(loggerSerilog).CreateLogger("SerilogTest");
        }

        public override void Dispose()
        {
            Serilog.Log.CloseAndFlush();
            base.Dispose();
        }
    }
}
