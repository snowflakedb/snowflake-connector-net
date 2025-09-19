using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Moq;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class FileCrlCacheConfigTest
    {
        [Test]
        [Platform("Win")]
        public void TestConfigForWindows()
        {
            // arrange
            const string WindowsHomeDirectory = "C:\\Windows\\Home";
            const string ExpectedCrlCacheDirectory = "C:\\Windows\\Home\\AppData\\Local\\Snowflake\\Caches\\crls";
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns(WindowsHomeDirectory);
            var unixOperations = new Mock<UnixOperations>();

            // act
            var config = new FileCrlCacheConfig(environmentOperations.Object, unixOperations.Object);

            // assert
            Assert.AreEqual(ExpectedCrlCacheDirectory, config.DirectoryPath);
            Assert.IsTrue(config.IsWindows);
            Assert.AreEqual(0, config.UnixUserId);
            Assert.AreEqual(0, config.UnixGroupId);
            unixOperations.VerifyNoOtherCalls();
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestConfigForMacOrLinux()
        {
            // arrange
            const long UnixUserId = 5;
            const long UnixGroupId = 6;
            const string HomeDirectory = "/Users/dotnet-user";
            const string ExpectedMacOsCrlCacheDirectory = "/Users/dotnet-user/Library/Caches/Snowflake/crls";
            const string ExpectedLinuxCrlCacheDirectory = "/Users/dotnet-user/.cache/snowflake/crls";
            var expectedDirectory = RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? ExpectedMacOsCrlCacheDirectory : ExpectedLinuxCrlCacheDirectory;
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns(HomeDirectory);
            var unixOperations = new Mock<UnixOperations>();
            unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(UnixUserId);
            unixOperations
                .Setup(u => u.GetCurrentGroupId())
                .Returns(UnixGroupId);

            // act
            var config = new FileCrlCacheConfig(environmentOperations.Object, unixOperations.Object);

            // assert
            Assert.AreEqual(expectedDirectory, config.DirectoryPath);
            Assert.IsFalse(config.IsWindows);
            Assert.AreEqual(UnixUserId, config.UnixUserId);
            Assert.AreEqual(UnixGroupId, config.UnixGroupId);
        }
    }
}
