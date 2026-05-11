using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class DirectoryOperationsTest : IDisposable
    {
        private readonly DirectoryOperations _directoryOperations;
        private readonly string _workingDirectory;
        private readonly string _dirAbsolutePath;

        public DirectoryOperationsTest()
        {
            var relativeWorkingDirectory = $"directory_operations_test_{Path.GetRandomFileName()}";
            _workingDirectory = Path.Combine(TempUtil.GetTempPath(), relativeWorkingDirectory);
            _dirAbsolutePath = Path.Combine(_workingDirectory, "testdir");

            if (!Directory.Exists(_workingDirectory))
            {
                Directory.CreateDirectory(_workingDirectory);
            }

            _directoryOperations = new DirectoryOperations();
        }

        public void Dispose()
        {
            if (Directory.Exists(_workingDirectory))
            {
                Directory.Delete(_workingDirectory, true);
            }
        }

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestDirectoryIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(_workingDirectory, "testdir");
            Directory.CreateDirectory(absoluteFilePath);

            // act and assert
            Assert.True(_directoryOperations.IsDirectorySafe(absoluteFilePath));
        }

        [TheorySkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        [MemberData(nameof(InsecurePermissionsData))]
        public void TestDirectoryIsNotSafeOnNotWindowsWhenPermissionsAreTooBroad(
            FileAccessPermissions permissions)
        {
            // arrange
            Syscall.mkdir(_dirAbsolutePath, (FilePermissions)permissions);

            // act and assert
            Assert.False(_directoryOperations.IsDirectorySafe(_dirAbsolutePath));
        }

        [Fact]
        public void TestShouldCreateDirectoryWithSafePermissions()
        {
            // act
            _directoryOperations.CreateDirectory(_dirAbsolutePath);

            // assert
            Assert.True(Directory.Exists(_dirAbsolutePath));
            Assert.True(_directoryOperations.IsDirectorySafe(_dirAbsolutePath));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestOwnerIsCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 1 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.True(dirOps.IsDirectoryOwnedByCurrentUser(_dirAbsolutePath));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestOwnerIsNotCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.False(dirOps.IsDirectoryOwnedByCurrentUser(_dirAbsolutePath));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestDirectoryIsNotSecureWhenNotOwnedByCurrentUser()
        {
            // arrange
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, DirectoryOwnerId = 2 };
            var dirOps = new DirectoryOperations(mockUnixOperations);

            // act and assert
            Assert.False(dirOps.IsDirectorySafe(_dirAbsolutePath));
        }

        // MemberData-compatible wrapper (returns IEnumerable<object[]>)
        public static IEnumerable<object[]> InsecurePermissionsData()
            => InsecurePermissions().Select(p => new object[] { p });

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
