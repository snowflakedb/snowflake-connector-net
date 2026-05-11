using System.Collections.Generic;
using System.IO;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class DirectoryOperationsTest
    {
        private static DirectoryOperations s_directoryOperations;
        private static readonly string s_relativeWorkingDirectory = $"directory_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_dirName = "testdir";
        private static readonly string s_dirAbsolutePath = Path.Combine(s_workingDirectory, s_dirName);
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_directoryOperations = new DirectoryOperations();
        }
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Fact]
        public void TestDirectoryIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_dirName);
            Directory.CreateDirectory(absoluteFilePath);

            // act and assert
            Assert.True(s_directoryOperations.IsDirectorySafe(absoluteFilePath));
        }

        [Fact]
        public void TestDirectoryIsNotSafeOnNotWindowsWhenPermissionsAreTooBroad(
            FileAccessPermissions permissions)
        {
            // arrange
            Syscall.mkdir(s_dirAbsolutePath, (FilePermissions)permissions);

            // act and assert
            Assert.False(s_directoryOperations.IsDirectorySafe(s_dirAbsolutePath));
        }

        [Fact]
        public void TestShouldCreateDirectoryWithSafePermissions()
        {
            // act
            s_directoryOperations.CreateDirectory(s_dirAbsolutePath);

            // assert
            Assert.True(Directory.Exists(s_dirAbsolutePath));
            Assert.True(s_directoryOperations.IsDirectorySafe(s_dirAbsolutePath));
        }

        [Fact]
        public void TestOwnerIsCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 1 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.True(dirOps.IsDirectoryOwnedByCurrentUser(s_dirAbsolutePath));
        }

        [Fact]
        public void TestOwnerIsNotCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.False(dirOps.IsDirectoryOwnedByCurrentUser(s_dirAbsolutePath));
        }

        [Fact]
        public void TestDirectoryIsNotSecureWhenNotOwnedByCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.False(dirOps.IsDirectorySafe(s_dirAbsolutePath));
        }

        // User permissions are required for all of the tests to be able to access directory information
        public static IEnumerable<FileAccessPermissions> InsecurePermissions()
        {
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead | FileAccessPermissions.GroupWrite;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupExecute;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupReadWriteExecute;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherRead | FileAccessPermissions.OtherWrite;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherExecute;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherReadWriteExecute;
            yield return FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.AllPermissions;
        }
    }
}
