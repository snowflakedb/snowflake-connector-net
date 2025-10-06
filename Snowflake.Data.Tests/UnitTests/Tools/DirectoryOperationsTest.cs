using System.Collections.Generic;
using System.IO;
using Mono.Unix;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture, NonParallelizable]
    public class DirectoryOperationsTest
    {
        private static DirectoryOperations s_directoryOperations;
        private static readonly string s_relativeWorkingDirectory = $"directory_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_dirName = "testdir";
        private static readonly string s_dirAbsolutePath = Path.Combine(s_workingDirectory, s_dirName);

        [SetUp]
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_directoryOperations = new DirectoryOperations();
        }

        [TearDown]
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        [Platform("Win")]
        public void TestDirectoryIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_dirName);
            Directory.CreateDirectory(absoluteFilePath);

            // act and assert
            Assert.IsTrue(s_directoryOperations.IsDirectorySafe(absoluteFilePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestDirectoryIsNotSafeOnNotWindowsWhenPermissionsAreTooBroad(
            [ValueSource(nameof(InsecurePermissions))]
            FileAccessPermissions permissions)
        {
            // arrange
            Syscall.mkdir(s_dirAbsolutePath, (FilePermissions)permissions);

            // act and assert
            Assert.IsFalse(s_directoryOperations.IsDirectorySafe(s_dirAbsolutePath));
        }

        [Test]
        public void TestShouldCreateDirectoryWithSafePermissions()
        {
            // act
            s_directoryOperations.CreateDirectory(s_dirAbsolutePath);

            // assert
            Assert.IsTrue(Directory.Exists(s_dirAbsolutePath));
            Assert.IsTrue(s_directoryOperations.IsDirectorySafe(s_dirAbsolutePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestOwnerIsCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 1 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.IsTrue(dirOps.IsDirectoryOwnedByCurrentUser(s_dirAbsolutePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestOwnerIsNotCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.IsFalse(dirOps.IsDirectoryOwnedByCurrentUser(s_dirAbsolutePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestDirectoryIsNotSecureWhenNotOwnedByCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.IsFalse(dirOps.IsDirectorySafe(s_dirAbsolutePath));
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
