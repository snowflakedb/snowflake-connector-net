using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.MiniCore;
using Snowflake.Data.Tests.IntegrationTests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests
{
    [Trait("Category", "MiniCore")]
    public class MiniCoreTestAsync : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;

        public MiniCoreTestAsync(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _originalMinicoreState = SFEnvironment.MinicoreDisabled;
            _fixture = fixture;
        }

        private readonly bool _originalMinicoreState;

        public void Dispose()
        {
            SFEnvironment.MinicoreDisabled = _originalMinicoreState;
        }

        private async Task WaitForMiniCoreToLoad()
        {
            SfMiniCore.StartLoading();
            for (int i = 0; i < 100 && !SfMiniCore.IsLoaded; i++)
                await Task.Delay(10);
        }

        [SFFact]
        public async Task TestMinicoreLoadsAndTelemetryIsCorrect()
        {
            await WaitForMiniCoreToLoad();

            var clientEnv = SFEnvironment.ClientEnv.CloneForSession();
            var loadError = SfMiniCore.GetLoadError();

            Assert.NotNull(clientEnv.minicoreVersion);
            Assert.Matches(@"^\d+\.\d+\.\d+", clientEnv.minicoreVersion);
            Assert.NotNull(clientEnv.minicoreFileName);
            Assert.Null(clientEnv.minicoreLoadError);
        }

        [SFFact]
        public void TestMinicoreIsDisabledInTelemetry()
        {
            SFEnvironment.MinicoreDisabled = true;
            var clientEnv = SFEnvironment.ClientEnv.CloneForSession();

            Assert.Null(clientEnv.minicoreVersion);
            Assert.Null(clientEnv.minicoreFileName);
            Assert.Equal(SfMiniCore.DISABLED_MESSAGE, clientEnv.minicoreLoadError);
        }

        [SFFact]
        public void TestGetExpectedLibraryNameReturnsCorrectName()
        {
            var name = SfMiniCore.GetExpectedLibraryName();

            Assert.NotNull(name);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                Assert.Equal("sf_mini_core.dll", name);
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                Assert.Equal("libsf_mini_core.dylib", name);
            else
                Assert.Equal("libsf_mini_core.so", name);
        }
}
}
