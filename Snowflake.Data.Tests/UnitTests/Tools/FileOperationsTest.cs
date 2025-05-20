using System.Collections.Generic;
using System.IO;
using System.Security;
using Mono.Unix;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture, NonParallelizable]
    public class FileOperationsTest
    {
        private static FileOperations s_fileOperations;
        private static readonly string s_relativeWorkingDirectory = $"file_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_content = "random text";
        private static readonly string s_fileName = "testfile";

        [SetUp]
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_fileOperations = new FileOperations();
        }

        [TearDown]
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        [Platform("Win")]
        public void TestReadAllTextOnWindows()
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.AreEqual(s_content, result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestReadAllTextCheckingPermissionsUsingTomlConfigurationFileValidations(
            [ValueSource(nameof(UserAllowedFilePermissions))]
            FileAccessPermissions userAllowedFilePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);
            var filePermissions = userAllowedFilePermissions;

            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.AreEqual(s_content, result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestShouldThrowExceptionIfOtherPermissionsIsSetWhenReadConfigurationFile(
            [ValueSource(nameof(UserAllowedFilePermissions))]
            FileAccessPermissions userAllowedFilePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);
            var filePermissions = userAllowedFilePermissions | FileAccessPermissions.OtherReadWriteExecute;

            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions),
                "Attempting to read a file with too broad permissions assigned");
        }


        [Test]
        [Platform(Exclude = "Win")]
        public void TestFileIsSafeOnNotWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            Syscall.creat(absoluteFilePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));

            // act and assert
            Assert.IsTrue(s_fileOperations.IsFileSafe(absoluteFilePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestFileIsNotSafeOnNotWindowsWhenTooBroadPermissionsAreUsed(
            [ValueSource(nameof(InsecurePermissions))]
            FileAccessPermissions permissions)
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            Syscall.creat(absoluteFilePath, (FilePermissions)permissions);

            // act and assert
            Assert.IsFalse(s_fileOperations.IsFileSafe(absoluteFilePath));
        }

        [Test]
        [Platform("Win")]
        public void TestFileIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            File.Create(absoluteFilePath).Close();

            // act and assert
            Assert.IsTrue(s_fileOperations.IsFileSafe(absoluteFilePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestOwnerIsCurrentUser()
        {
            // arrange
            var absolutePath = Path.Combine(s_workingDirectory, s_fileName);
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 1 };
            var fileOps = new FileOperations(mockUnixOperations);

            // act and assert
            Assert.IsTrue(fileOps.IsFileOwnedByCurrentUser(absolutePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestOwnerIsNotCurrentUser()
        {
            // arrange
            var absolutePath = Path.Combine(s_workingDirectory, s_fileName);
            var mockUnixOperations = new MockUnixOperations { CurrentUserId = 1, FileOwnerId = 2 };
            var fileOps = new FileOperations(mockUnixOperations);

            // act and assert
            Assert.IsFalse(fileOps.IsFileOwnedByCurrentUser(absolutePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
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
                Assert.IsFalse(fileOps.IsFileSafe(absolutePath));
            }
            finally
            {
                File.Delete(absolutePath);
            }
        }

        [Test]
        [Platform(Exclude = "Win")]
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
            Assert.IsTrue(File.Exists(DestFilePath));
            Assert.IsTrue(s_fileOperations.IsFileSafe(DestFilePath));
            Assert.AreEqual(s_content, File.ReadAllText(DestFilePath));
        }

        [Test]
        [Platform(Exclude = "Win")]
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
            Assert.Throws<SecurityException>(() => s_fileOperations.CopyFile(SrcFilePath, DestFilePath), $"File ${SrcFilePath} is not safe to read.");
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
    }
}
