using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.MiniCore;

namespace Snowflake.Data.Tests
{
    [TestFixture]
    [Category("MiniCore")]
    public class MiniCoreTest : SFBaseTest
    {
        private void WaitForMiniCoreToLoad()
        {
            SfMiniCore.StartLoading();
            for (int i = 0; i < 100 && !SfMiniCore.IsLoaded; i++)
                Thread.Sleep(10);
        }

        [Test]
        public void TestMinicoreLoadsAndTelemetryIsCorrect()
        {
            WaitForMiniCoreToLoad();

            var clientEnv = SFEnvironment.ClientEnv.CloneForSession();
            var loadError = SfMiniCore.GetLoadError();

            Assert.IsNotNull(clientEnv.minicoreVersion,
                $"minicoreVersion should not be null. LoadError: {loadError}, FileName: {clientEnv.minicoreFileName}");
            Assert.That(clientEnv.minicoreVersion, Does.Match(@"^\d+\.\d+\.\d+"),
                $"Version should be semver, got: {clientEnv.minicoreVersion}");
            Assert.IsNotNull(clientEnv.minicoreFileName, "minicoreFileName should not be null");
            Assert.IsNull(clientEnv.minicoreLoadError, $"minicoreLoadError should be null on success, got: {clientEnv.minicoreLoadError}");
        }

        [Test]
        public void TestGetExpectedLibraryNameReturnsCorrectName()
        {
            var name = SfMiniCore.GetExpectedLibraryName();

            Assert.IsNotNull(name);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                Assert.AreEqual("sf_mini_core.dll", name);
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                Assert.AreEqual("libsf_mini_core.dylib", name);
            else
                Assert.AreEqual("libsf_mini_core.so", name);
        }

    }
}
