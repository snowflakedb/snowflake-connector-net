using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
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

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(UserAllowedFilePermissionsData))]
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

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(UserAllowedFilePermissionsData))]
        public void TestShouldThrowExceptionIfOtherPermissionsIsSetWhenReadConfigurationFile(
            FileAccessPermissions userAllowedFilePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);
            var filePermissions = userAllowedFilePermissions | FileAccessPermissions.OtherReadWriteExecute;

            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
        }


        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestFileCopyHappyPath()
        {
            // arrange
            const string SrcFile = "srcfile";
            var srcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile";
            var destFilePath = Path.Combine(s_workingDirectory, DestFile);

            s_fileOperations.Create(srcFilePath).Close();
            File.WriteAllText(srcFilePath, s_content);

            // act
            s_fileOperations.CopyFile(srcFilePath, destFilePath);

            // assert
            Assert.True(File.Exists(destFilePath));
            Assert.Equal(s_content, File.ReadAllText(destFilePath));
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(InsecurePermissionsData))]
        public void TestFileCopyShouldThrowExceptionIfTooBroadPermissionsAreUsed(
            FileAccessPermissions permissions)
        {
            // arrange
            const string SrcFile = "srcfile";
            var srcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile";
            var destFilePath = Path.Combine(s_workingDirectory, DestFile);

            s_fileOperations.Create(srcFilePath).Close();
            File.WriteAllText(srcFilePath, s_content);
            Syscall.chmod(srcFilePath, (FilePermissions)(permissions | FileAccessPermissions.UserRead));

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.CopyFile(srcFilePath, destFilePath));
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestFileCopyShouldThrowWhenFileNotOwnedByCurrentUser()
        {
            // arrange - mock returns a different uid than the real file owner
            const string SrcFile = "srcfile_owner";
            var srcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile_owner";
            var destFilePath = Path.Combine(s_workingDirectory, DestFile);

            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 2 };
            var fileOps = new FileOperations(mockUnixOperations);
            fileOps.Create(srcFilePath).Close();
            try
            {
                File.WriteAllText(srcFilePath, s_content);
                Assert.Throws<SecurityException>(() => fileOps.CopyFile(srcFilePath, destFilePath));
            }
            finally
            {
                File.Delete(srcFilePath);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(InsecurePermissionsData))]
        public void TestFileCopyRejectsInsecurePermissionsViaFileDescriptor(FileAccessPermissions permissions)
        {
            // Validates the TOCTOU fix: permission check happens on the open fd,
            // not via a separate stat() call on the path.
            var srcFilePath = Path.Combine(s_workingDirectory, $"srcfile_fd_{permissions}");
            var destFilePath = Path.Combine(s_workingDirectory, $"destfile_fd_{permissions}");

            s_fileOperations.Create(srcFilePath).Close();
            File.WriteAllText(srcFilePath, s_content);
            Syscall.chmod(srcFilePath, (FilePermissions)(permissions | FileAccessPermissions.UserRead));

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.CopyFile(srcFilePath, destFilePath));
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestFileCopySucceedsWithSafePermissions()
        {
            const string SrcFile = "srcfile_safe";
            var srcFilePath = Path.Combine(s_workingDirectory, SrcFile);
            const string DestFile = "destfile_safe";
            var destFilePath = Path.Combine(s_workingDirectory, DestFile);

            s_fileOperations.Create(srcFilePath).Close();
            File.WriteAllText(srcFilePath, s_content);

            // act
            s_fileOperations.CopyFile(srcFilePath, destFilePath);

            // assert
            Assert.True(File.Exists(destFilePath));
            Assert.Equal(s_content, File.ReadAllText(destFilePath));
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

        public static IEnumerable<object[]> UserAllowedFilePermissionsData() =>
            UserAllowedFilePermissions().Select(x => new object[] { x });

        public static IEnumerable<object[]> InsecurePermissionsData() =>
            InsecurePermissions().Select(x => new object[] { x });

        public void Dispose()
        {
            After();
        }
    }
}
