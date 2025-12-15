using System.IO;
using Mono.Unix;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    [Platform(Exclude = "Win")]
    public class DirectoryUnixInformationTest
    {
        private const long UserId = 5;
        private const long AnotherUserId = 6;
        static readonly string s_directoryFullName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        [Test]
        [TestCase(FileAccessPermissions.UserWrite)]
        [TestCase(FileAccessPermissions.UserRead)]
        [TestCase(FileAccessPermissions.UserExecute)]
        [TestCase(FileAccessPermissions.UserReadWriteExecute)]
        public void TestSafeDirectory(FileAccessPermissions securePermissions)
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, securePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafe(UserId);

            // assert
            Assert.True(isSafe);
        }

        [Test]
        [TestCase(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead)]
        [TestCase(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead)]
        public void TestUnsafePermissions(FileAccessPermissions unsecurePermissions)
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, unsecurePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafe(UserId);

            // assert
            Assert.False(isSafe);
        }

        [Test]
        public void TestSafeExactlyDirectory()
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, FileAccessPermissions.UserReadWriteExecute, UserId);

            // act
            var isSafe = dirInfo.IsSafeExactly(UserId);

            // assert
            Assert.True(isSafe);
        }

        [Test]
        [TestCase(FileAccessPermissions.UserRead)]
        [TestCase(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead)]
        [TestCase(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead)]
        public void TestUnsafeExactlyPermissions(FileAccessPermissions unsecurePermissions)
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, unsecurePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafeExactly(UserId);

            // assert
            Assert.False(isSafe);
        }

        [Test]
        public void TestOwnedByOthers()
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, FileAccessPermissions.UserReadWriteExecute, UserId);

            // act
            var isSafe = dirInfo.IsSafe(AnotherUserId);
            var isSafeExactly = dirInfo.IsSafeExactly(AnotherUserId);

            // assert
            Assert.False(isSafe);
            Assert.False(isSafeExactly);
        }
    }
}
