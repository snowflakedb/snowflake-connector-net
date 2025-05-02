using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using Mono.Unix;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture, NonParallelizable]
    public class UnixOperationsTest
    {
        private static UnixOperations s_unixOperations;
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        [OneTimeSetUp]
        public static void BeforeAll()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
            s_unixOperations = new UnixOperations();
        }

        [OneTimeTearDown]
        public static void AfterAll()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestDetectGroupOrOthersWritablePermissions(
            [ValueSource(nameof(GroupOrOthersWritablePermissions))] FileAccessPermissions groupOrOthersWritablePermissions,
            [ValueSource(nameof(GroupNotWritablePermissions))] FileAccessPermissions groupNotWritablePermissions,
            [ValueSource(nameof(OtherNotWritablePermissions))] FileAccessPermissions otherNotWritablePermissions)
        {
            // arrange
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var readWriteUserPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
            var filePermissions = readWriteUserPermissions | groupOrOthersWritablePermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions) filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.IsTrue(result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestDetectGroupOrOthersNotWritablePermissions(
            [ValueSource(nameof(UserPermissions))] FileAccessPermissions userPermissions,
            [ValueSource(nameof(GroupNotWritablePermissions))] FileAccessPermissions groupNotWritablePermissions,
            [ValueSource(nameof(OtherNotWritablePermissions))] FileAccessPermissions otherNotWritablePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var filePermissions = userPermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions) filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.IsFalse(result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestReadAllTextCheckingPermissionsUsingTomlConfigurationFileValidations(
            [ValueSource(nameof(UserAllowedPermissions))] FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions) userAllowedPermissions);

            // act
            var result = s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.AreEqual(content, result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestWriteAllTextCheckingPermissionsUsingSFCredentialManagerFileValidations(
            [ValueSource(nameof(UserAllowedWritePermissions))] FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions) userAllowedPermissions);

            // act and assert
            Assert.DoesNotThrow(() => s_unixOperations.WriteAllText(filePath,"test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWithTomlConfigurationValidations([ValueSource(nameof(UserReadWritePermissions))] FileAccessPermissions userPermissions,
            [ValueSource(nameof(GroupPermissions))] FileAccessPermissions groupPermissions,
            [ValueSource(nameof(OthersPermissions))] FileAccessPermissions othersPermissions)
        {
            if(groupPermissions == 0 && othersPermissions == 0)
            {
                Assert.Ignore("Skip test when group and others have no permissions");
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions) filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions), "Attempting to read a file with too broad permissions assigned");
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForCredentialManagerFile([ValueSource(nameof(UserReadWritePermissions))] FileAccessPermissions userPermissions,
            [ValueSource(nameof(GroupPermissions))] FileAccessPermissions groupPermissions,
            [ValueSource(nameof(OthersPermissions))] FileAccessPermissions othersPermissions)
        {
            if(groupPermissions == 0 && othersPermissions == 0)
            {
                Assert.Ignore("Skip test when group and others have no permissions");
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions) filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_unixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions), "Attempting to read or write a file with too broad permissions assigned");
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestCreateFileWithUserRwPermissions()
        {
            // arrange
            var filePath = Path.Combine(s_workingDirectory, "testfile");

            // act
            s_unixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

            // assert
            var fileInfo = new UnixFileInfo(filePath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));
            Assert.IsFalse(result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestCreateDirectoryWithUserRwxPermissions()
        {
            // arrange
            var dirPath = Path.Combine(s_workingDirectory, "testdir");

            // act
            s_unixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // assert
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);
            Assert.IsFalse(result);
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestNestedDir()
        {
            // arrange
            var dirPath = Path.Combine(s_workingDirectory, "testdir", "a", "b", "c");
            s_unixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // act
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);

            // assert
            Assert.IsFalse(result);
        }

        public static IEnumerable<FileAccessPermissions> UserPermissions()
        {
            yield return FileAccessPermissions.UserRead;
            yield return FileAccessPermissions.UserWrite;
            yield return FileAccessPermissions.UserExecute;
            yield return FileAccessPermissions.UserReadWriteExecute;
        }

        public static IEnumerable<FileAccessPermissions> GroupPermissions()
        {
            yield return 0;
            yield return FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.GroupWrite;
            yield return FileAccessPermissions.GroupExecute;
            yield return FileAccessPermissions.GroupReadWriteExecute;
        }

        public static IEnumerable<FileAccessPermissions> OthersPermissions()
        {
            yield return 0;
            yield return FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.GroupWrite;
            yield return FileAccessPermissions.GroupExecute;
            yield return FileAccessPermissions.GroupReadWriteExecute;
        }

        public static IEnumerable<FileAccessPermissions> GroupOrOthersWritablePermissions()
        {
            yield return FileAccessPermissions.GroupWrite;
            yield return FileAccessPermissions.OtherWrite;
            yield return FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite;
        }

        public static IEnumerable<FileAccessPermissions> GroupNotWritablePermissions()
        {
            yield return 0;
            yield return FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.GroupExecute;
            yield return FileAccessPermissions.GroupRead | FileAccessPermissions.GroupExecute;
        }

        public static IEnumerable<FileAccessPermissions> OtherNotWritablePermissions()
        {
            yield return 0;
            yield return FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.OtherExecute;
            yield return FileAccessPermissions.OtherRead | FileAccessPermissions.OtherExecute;
        }

        public static IEnumerable<FileAccessPermissions> UserReadWritePermissions()
        {
            yield return FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
        }

        public static IEnumerable<FileAccessPermissions> UserAllowedPermissions()
        {
            yield return FileAccessPermissions.UserRead;
            yield return FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
        }

        public static IEnumerable<FileAccessPermissions> UserAllowedWritePermissions()
        {
            yield return FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
        }

        public static IEnumerable<FileAccessPermissions> GroupOrOthersReadablePermissions()
        {
            yield return 0;
            yield return FileAccessPermissions.GroupRead;
            yield return FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
        }
    }
}
