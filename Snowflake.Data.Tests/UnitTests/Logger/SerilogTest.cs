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
        private readonly Serilog.Core.Logger _serilogLogger;

        public SerilogTest()
        {
            _logFile = SerilogFileName;

            _serilogLogger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.File(SerilogFileName, flushToDiskInterval: TimeSpan.Zero)
                .CreateLogger();

            _customLogger = new SerilogLoggerFactory(_serilogLogger).CreateLogger("SerilogTest");
        }

        public override void Dispose()
        {
            _serilogLogger.Dispose(); // Release the file handle before base.Dispose() deletes the file
            base.Dispose();
        }
    }
}
