using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class FileOperationsUnixTest : IDisposable
    {
        public FileOperationsUnixTest()
        {
            Before();
        }

        private static FileOperations s_fileOperations;
        private static readonly string s_relativeWorkingDirectory = $"file_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_content = "random text";
        private static readonly string s_fileName = "testfile";
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_fileOperations = new FileOperations();
        }
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Fact]
        public void TestReadAllTextCheckingPermissionsUsingTomlConfigurationFileValidations(
            FileAccessPermissions userAllowedFilePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);
            var filePermissions = userAllowedFilePermissions;

            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(s_content, result);
        }

        [Fact]
        public void TestShouldThrowExceptionIfOtherPermissionsIsSetWhenReadConfigurationFile(
            FileAccessPermissions userAllowedFilePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);
            var filePermissions = userAllowedFilePermissions | FileAccessPermissions.OtherReadWriteExecute;

            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
        }


        [Fact]
        public void TestFileIsSafeOnNotWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            Syscall.creat(absoluteFilePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));

            // act and assert
            Assert.True(s_fileOperations.IsFileSafe(absoluteFilePath));
        }

        [Fact]
        public void TestFileIsNotSafeOnNotWindowsWhenTooBroadPermissionsAreUsed(
            FileAccessPermissions permissions)
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            Syscall.creat(absoluteFilePath, (FilePermissions)permissions);

            // act and assert
            Assert.False(s_fileOperations.IsFileSafe(absoluteFilePath));
        }

        [Fact]
        public void TestOwnerIsCurrentUser()
        {
            // arrange
            var absolutePath = Path.Combine(s_workingDirectory, s_fileName);
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 1 };
            var fileOps = new FileOperations(mockUnixOperations);

            // act and assert
            Assert.True(fileOps.IsFileOwnedByCurrentUser(absolutePath));
        }

        [Fact]
        public void TestOwnerIsNotCurrentUser()
        {
            // arrange
            var absolutePath = Path.Combine(s_workingDirectory, s_fileName);
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 2 };
            var fileOps = new FileOperations(mockUnixOperations);

            // act and assert
            Assert.False(fileOps.IsFileOwnedByCurrentUser(absolutePath));
        }

        [Fact]
        public void TestFileIsNotSecureWhenNotOwnedByCurrentUser()
        {
            // arrange
            var absolutePath = Path.Combine(s_workingDirectory, s_fileName);
            File.Create(absolutePath);
            try
            {
                var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 2 };
                var fileOps = new FileOperations(mockUnixOperations);

                // act and assert
                Assert.False(fileOps.IsFileSafe(absolutePath));
            }
            finally
            {
                File.Delete(absolutePath);
            }
        }

        [Fact]
        public void TestFileCopyUsesProperPermissions()
        {
            // arrange
            const string SrcFile = "srcfile";
            var SrcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile";
            var DestFilePath = Path.Combine(s_workingDirectory, DestFile);

            s_fileOperations.Create(SrcFilePath).Close();
            File.WriteAllText(SrcFilePath, s_content);

            // act
            s_fileOperations.CopyFile(SrcFilePath, DestFilePath);

            // assert
            Assert.True(File.Exists(DestFilePath));
            Assert.True(s_fileOperations.IsFileSafe(DestFilePath));
            Assert.Equal(s_content, File.ReadAllText(DestFilePath));
        }

        [Fact]
        public void TestFileCopyShouldThrowExecptionIfTooBroadPermissionsAreUsed()
        {
            // arrange
            const string SrcFile = "srcfile";
            var SrcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile";
            var DestFilePath = Path.Combine(s_workingDirectory, DestFile);

            s_fileOperations.Create(SrcFilePath).Close();
            Syscall.chmod(SrcFilePath, (FilePermissions)FileAccessPermissions.AllPermissions);
            File.WriteAllText(SrcFilePath, s_content);

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.CopyFile(SrcFilePath, DestFilePath));
        }


        public static IEnumerable<FileAccessPermissions> UserAllowedFilePermissions()
        {
            yield return FileAccessPermissions.UserRead;
            yield return FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
        }

        public static IEnumerable<FileAccessPermissions> InsecurePermissions()
        {
            yield return FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.GroupRead | FileAccessPermissions.GroupWrite;
            yield return FileAccessPermissions.OtherRead | FileAccessPermissions.OtherWrite;
            yield return FileAccessPermissions.GroupRead | FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherRead | FileAccessPermissions.OtherWrite;
            yield return FileAccessPermissions.GroupReadWriteExecute;
            yield return FileAccessPermissions.OtherReadWriteExecute;
            yield return FileAccessPermissions.GroupReadWriteExecute | FileAccessPermissions.OtherReadWriteExecute;
            yield return FileAccessPermissions.AllPermissions;
        }
    
        public void Dispose()
        {
            After();
        }
}
}
