using System;
using System.IO;
using Mono.Unix;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class DirectoryUnixInformationTest
    {
        private const long UserId = 5;
        private const long AnotherUserId = 6;
        static readonly string s_directoryFullName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        [SFTheory(SkipCondition.SkipOnWindows)]
        [InlineData(FileAccessPermissions.UserWrite)]
        [InlineData(FileAccessPermissions.UserRead)]
        [InlineData(FileAccessPermissions.UserExecute)]
        [InlineData(FileAccessPermissions.UserReadWriteExecute)]
        public void TestSafeDirectory(FileAccessPermissions securePermissions)
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, securePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafe(UserId);

            // assert
            Assert.True(isSafe);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [InlineData(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead, "User RW + Group R")]
        [InlineData(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead, "User RWX + Other R")]
        public void TestUnsafePermissions(FileAccessPermissions unsecurePermissions, string log)
        {
            // arrange
            Console.WriteLine($@"Executing {nameof(TestUnsafePermissions)} with {log}..");
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, unsecurePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafe(UserId);

            // assert
            Assert.False(isSafe);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestSafeExactlyDirectory()
        {
            // arrange
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, FileAccessPermissions.UserReadWriteExecute, UserId);

            // act
            var isSafe = dirInfo.IsSafeExactly(UserId);

            // assert
            Assert.True(isSafe);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [InlineData(FileAccessPermissions.UserRead, "User R")]
        [InlineData(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead, "User RWX + Group R")]
        [InlineData(FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead, "User RWX + Other R")]
        public void TestUnsafeExactlyPermissions(FileAccessPermissions unsecurePermissions, string log)
        {
            // arrange
            Console.WriteLine($@"Executing {nameof(TestUnsafePermissions)} with {log}..");
            var dirInfo = new DirectoryUnixInformation(s_directoryFullName, true, unsecurePermissions, UserId);

            // act
            var isSafe = dirInfo.IsSafeExactly(UserId);

            // assert
            Assert.False(isSafe);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
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
