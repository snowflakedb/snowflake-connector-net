using System;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.MiniCore;

namespace Snowflake.Data.Tests
{
    [TestFixture]
    public class MiniCoreTest : SFBaseTest
    {
        [Test]
        [Category("MiniCore")]
        public void TestMinicoreLoadsSuccessfully()
        {
            // MiniCore library should be available in runtimes/ directory
            // and loaded automatically via DllImport

            var clientEnv = SFEnvironment.ClientEnv;

            Assert.IsNotNull(clientEnv, "ClientEnv should not be null");
            Assert.IsNotNull(clientEnv.minicoreVersion, "minicoreVersion should not be null");

            // Should NOT contain any error
            Assert.That(clientEnv.minicoreVersion, Does.Not.StartWith("ERROR"),
                $"MiniCore should load successfully, but got: {clientEnv.minicoreVersion}");
            Assert.That(clientEnv.minicoreVersion, Does.Not.StartWith("LIBRARY NOT FOUND"),
                $"MiniCore library should be found in runtimes/, but got: {clientEnv.minicoreVersion}");

            // Should be semantic version (e.g., "0.0.1")
            Assert.That(clientEnv.minicoreVersion, Does.Match(@"^\d+\.\d+\.\d+"),
                $"MiniCore version should follow semantic versioning pattern, but got: {clientEnv.minicoreVersion}");
        }

        [Test]
        [Category("MiniCore")]
        public void TestMinicoreDirectCallReturnsVersion()
        {
            // Direct call to SfMiniCore.GetFullVersion() should return valid version

            var version = SfMiniCore.GetFullVersion();

            Assert.IsNotNull(version, "GetFullVersion() should not return null");
            Assert.IsNotEmpty(version, "GetFullVersion() should not return empty string");

            // Should be semantic version
            Assert.That(version, Does.Match(@"^\d+\.\d+\.\d+"),
                $"Version should follow semantic versioning pattern, but got: {version}");
        }

        [Test]
        [Category("MiniCore")]
        public void TestMinicoreVersionConsistency()
        {
            var version1 = SFEnvironment.ClientEnv.minicoreVersion;
            var version2 = SFEnvironment.ClientEnv.minicoreVersion;
            var version3 = SfMiniCore.TryGetVersionSafe();
            var version4 = SfMiniCore.GetFullVersion();

            Assert.AreEqual(version1, version2,
                "Multiple accesses to ClientEnv.minicoreVersion should return the same value");
            Assert.AreEqual(version1, version3,
                "ClientEnv.minicoreVersion should match TryGetVersionSafe()");
            Assert.AreEqual(version1, version4,
                "ClientEnv.minicoreVersion should match direct GetFullVersion() call");
        }

        [Test]
        [Category("MiniCore")]
        public void TestMinicoreVersionIncludedInClientEnvironment()
        {
            // Verify that MiniCore version is included in the client environment
            // sent during authentication

            var clientEnv = SFEnvironment.ClientEnv;

            Assert.IsNotNull(clientEnv.minicoreVersion,
                "minicoreVersion should be populated in ClientEnv for authentication");

            // Version should be valid (not an error)
            Assert.That(clientEnv.minicoreVersion, Does.Match(@"^\d+\.\d+\.\d+"),
                "minicoreVersion in ClientEnv should be a valid semantic version");
        }
    }
}

