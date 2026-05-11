using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Snowflake.Data.Logger;
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class UnixOperationsTest
    {
        private static UnixOperations s_unixOperations;
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        public static void BeforeAll()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
            s_unixOperations = new UnixOperations();
        }
        public static void AfterAll()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestDetectGroupOrOthersWritablePermissions(
            FileAccessPermissions groupOrOthersWritablePermissions,
            FileAccessPermissions groupNotWritablePermissions,
            FileAccessPermissions otherNotWritablePermissions)
        {
            // arrange
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var readWriteUserPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
            var filePermissions = readWriteUserPermissions | groupOrOthersWritablePermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.True(result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestDetectGroupOrOthersNotWritablePermissions(
            FileAccessPermissions userPermissions,
            FileAccessPermissions groupNotWritablePermissions,
            FileAccessPermissions otherNotWritablePermissions)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var filePermissions = userPermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.False(result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReadAllTextCheckingPermissionsUsingTomlConfigurationFileValidations(
            FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions)userAllowedPermissions);

            // act
            var result = s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestSkipReadPermissionsWhenSkipIsEnabled()
        {
            var logsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var logPath = Path.Combine(logsDirectory, $"easy_logging_logs_{Path.GetRandomFileName()}", "dotnet");
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "true");
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, logPath);

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = FileAccessPermissions.UserWrite | FileAccessPermissions.UserRead |
                FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
            var logLines = File.ReadLines(Logger.EasyLoggerManagerTest.FindLogFilePath(logPath));
            Assert.Equal(0, logLines.Count(s => s.Contains("File is readable by someone other than the owner")));

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestCheckReadPermissionsWhenSkipIsDisabled()
        {
            var logsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var logPath = Path.Combine(logsDirectory, $"easy_logging_logs_{Path.GetRandomFileName()}", "dotnet");
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, logPath);

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = FileAccessPermissions.UserWrite | FileAccessPermissions.UserRead |
                FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
            var logLines = File.ReadLines(Logger.EasyLoggerManagerTest.FindLogFilePath(logPath));
            Assert.Equal(1, logLines.Count(s => s.Contains("File is readable by someone other than the owner")));

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
        }

        [TheorySkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        [InlineData("true", false)]
        [InlineData("false", true)]
        public void TestTomlPermissionChecksWithSkipTokenFileVerification(string skipValue, bool shouldThrow)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            Syscall.chmod(filePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupWrite));
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, skipValue);

            if (shouldThrow)
                Assert.Throws<SecurityException>(() => s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
            else
                s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, null);
        }

        [TheorySkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        [InlineData("true", false)]
        [InlineData("false", true)]
        public void TestCredentialManagerPermissionChecksWithSkipTokenFileVerification(string skipValue, bool shouldThrow)
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            Syscall.chmod(filePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupWrite));
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, skipValue);

            if (shouldThrow)
                Assert.Throws<SecurityException>(() => s_unixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions));
            else
                s_unixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions);

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, null);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestWriteAllTextCheckingPermissionsUsingSFCredentialManagerFileValidations(
            FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions)userAllowedPermissions);

            // act and assert
            s_unixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWithTomlConfigurationValidations(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                Skip.If(true, "Skip test when group and others have no permissions");
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            if ((groupPermissions & (FileAccessPermissions.GroupWrite | FileAccessPermissions.GroupExecute)) != 0 ||
                (othersPermissions & (FileAccessPermissions.OtherWrite | FileAccessPermissions.OtherExecute)) != 0)
                Assert.Throws<SecurityException>(() => s_unixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForCredentialManagerFile(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                Skip.If(true, "Skip test when group and others have no permissions");
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_unixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForLogFile(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                Skip.If(true, "Skip test when group and others have no permissions");
            }
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_unixOperations.WriteAllText(filePath, "test", EasyLoggerValidator.Instance.ValidateLogFilePermissions));
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestCreateFileWithUserRwPermissions()
        {
            // arrange
            var filePath = Path.Combine(s_workingDirectory, "testfile");

            // act
            s_unixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

            // assert
            var fileInfo = new UnixFileInfo(filePath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));
            Assert.False(result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestCreateDirectoryWithUserRwxPermissions()
        {
            // arrange
            var dirPath = Path.Combine(s_workingDirectory, "testdir");

            // act
            s_unixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // assert
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);
            Assert.False(result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestNestedDir()
        {
            // arrange
            var dirPath = Path.Combine(s_workingDirectory, "testdir", "a", "b", "c");
            s_unixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // act
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);

            // assert
            Assert.False(result);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReadBytesFromEmptyFile()
        {
            // arrange
            var filePath = Path.Combine(s_workingDirectory, $"empty_file_{Path.GetRandomFileName()}");
            s_unixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

            // act
            var bytes = s_unixOperations.ReadAllBytes(filePath, s => { });

            // assert
            Assert.Equal(0, bytes.Length);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReadBytesFromSmallFile()
        {
            // arrange
            var randomBytes = TestDataGenarator.NextBytes(19);
            var filePath = Path.Combine(s_workingDirectory);
            s_unixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            s_unixOperations.WriteAllBytes(filePath, randomBytes, _ => { });

            // act
            var bytes = s_unixOperations.ReadAllBytes(filePath, s => { });

            // assert
            Assert.Equal(randomBytes, bytes);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReadBytesFromLargeFile()
        {
            // arrange
            var filePath = Path.Combine("crl");
            var expectedBytes = File.ReadAllBytes(filePath);

            // act
            var bytes = s_unixOperations.ReadAllBytes(filePath, s => { });

            // assert
            Assert.Equal(expectedBytes, bytes);
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
            yield return FileAccessPermissions.OtherRead;
            yield return FileAccessPermissions.OtherWrite;
            yield return FileAccessPermissions.OtherExecute;
            yield return FileAccessPermissions.OtherReadWriteExecute;
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
