using System;
using System.Runtime.InteropServices;
using Xunit;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Moq;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    public class FileCrlCacheConfigTest
    {
        [SFFact(SkipCondition.RunOnlyOnWindows)]
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
            Assert.Equal(ExpectedCrlCacheDirectory, config.DirectoryPath);
            Assert.True(config.IsWindows);
            Assert.Equal(0, config.UnixUserId);
            Assert.Equal(0, config.UnixGroupId);
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact(SkipCondition.SkipOnWindows)]
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
            Assert.Equal(expectedDirectory, config.DirectoryPath);
            Assert.False(config.IsWindows);
            Assert.Equal(UnixUserId, config.UnixUserId);
            Assert.Equal(UnixGroupId, config.UnixGroupId);
        }
    }
}
