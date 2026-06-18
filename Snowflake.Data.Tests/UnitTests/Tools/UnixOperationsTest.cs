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
    [CollectionDefinition(nameof(UnixOperationsTestFixture), DisableParallelization = true)]
    public sealed class UnixOperationsTestFixture : ICollectionFixture<UnixOperationsTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            public string WorkingDirectory { get; }
            internal UnixOperations UnixOperations { get; }

            public Fixture()
            {
                WorkingDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                if (!Directory.Exists(WorkingDirectory))
                {
                    Directory.CreateDirectory(WorkingDirectory);
                }
                UnixOperations = new UnixOperations();
            }

            public void Dispose()
            {
                Directory.Delete(WorkingDirectory, true);
            }
        }
    }

    [Collection(nameof(UnixOperationsTestFixture))]
    public class UnixOperationsTest
    {
        private readonly UnixOperationsTestFixture.Fixture _fixture;

        public UnixOperationsTest(UnixOperationsTestFixture.Fixture fixture)
        {
            _fixture = fixture;
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(TestDetectGroupOrOthersWritablePermissionsData))]
        public void TestDetectGroupOrOthersWritablePermissions(
            FileAccessPermissions groupOrOthersWritablePermissions,
            FileAccessPermissions groupNotWritablePermissions,
            FileAccessPermissions otherNotWritablePermissions)
        {
            // arrange
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, "random text");
            var readWriteUserPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
            var filePermissions = readWriteUserPermissions | groupOrOthersWritablePermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = _fixture.UnixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.True(result);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(TestDetectGroupOrOthersNotWritablePermissionsData))]
        public void TestDetectGroupOrOthersNotWritablePermissions(
            FileAccessPermissions userPermissions,
            FileAccessPermissions groupNotWritablePermissions,
            FileAccessPermissions otherNotWritablePermissions)
        {
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, "random text");
            var filePermissions = userPermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);
            var fileInfo = new UnixFileInfo(filePath);

            // act
            var result = _fixture.UnixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.False(result);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(UserAllowedPermissionsData))]
        public void TestReadAllTextCheckingPermissionsUsingTomlConfigurationFileValidations(
            FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions)userAllowedPermissions);

            // act
            var result = _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestSkipReadPermissionsWhenSkipIsEnabled()
        {
            var logsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var logPath = Path.Combine(logsDirectory, $"easy_logging_logs_{Path.GetRandomFileName()}", "dotnet");
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "true");
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, logPath);

            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);

            var filePermissions = FileAccessPermissions.UserWrite | FileAccessPermissions.UserRead |
                FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
            var logLines = File.ReadLines(Logger.EasyLoggerManagerTest.FindLogFilePath(logPath));
            Assert.Equal(0, logLines.Count(s => s.Contains("File is readable by someone other than the owner")));

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestCheckReadPermissionsWhenSkipIsDisabled()
        {
            var logsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var logPath = Path.Combine(logsDirectory, $"easy_logging_logs_{Path.GetRandomFileName()}", "dotnet");
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, logPath);

            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);

            var filePermissions = FileAccessPermissions.UserWrite | FileAccessPermissions.UserRead |
                FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(content, result);
            var logLines = File.ReadLines(Logger.EasyLoggerManagerTest.FindLogFilePath(logPath));
            Assert.Equal(1, logLines.Count(s => s.Contains("File is readable by someone other than the owner")));

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipWarningForReadPermissions, "false");
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [InlineData("true", false)]
        [InlineData("false", true)]
        public void TestTomlPermissionChecksWithSkipTokenFileVerification(string skipValue, bool shouldThrow)
        {
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, "random text");
            Syscall.chmod(filePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupWrite));
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, skipValue);

            if (shouldThrow)
                Assert.Throws<SecurityException>(() => _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
            else
                _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, null);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [InlineData("true", false)]
        [InlineData("false", true)]
        public void TestCredentialManagerPermissionChecksWithSkipTokenFileVerification(string skipValue, bool shouldThrow)
        {
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, "random text");
            Syscall.chmod(filePath, (FilePermissions)(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupWrite));
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, skipValue);

            if (shouldThrow)
                Assert.Throws<SecurityException>(() => _fixture.UnixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions));
            else
                _fixture.UnixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions);

            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SkipTokenFilePermissionsVerification, null);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(UserAllowedWritePermissionsData))]
        public void TestWriteAllTextCheckingPermissionsUsingSFCredentialManagerFileValidations(
            FileAccessPermissions userAllowedPermissions)
        {
            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);
            Syscall.chmod(filePath, (FilePermissions)userAllowedPermissions);

            // act and assert
            _fixture.UnixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions);
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(TestFailIfGroupOrOthersHavePermissionsToFileWithTomlConfigurationValidationsData))]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWithTomlConfigurationValidations(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                // Skip test when group and others have no permissions
                return;
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            if ((groupPermissions & (FileAccessPermissions.GroupWrite | FileAccessPermissions.GroupExecute)) != 0 ||
                (othersPermissions & (FileAccessPermissions.OtherWrite | FileAccessPermissions.OtherExecute)) != 0)
                Assert.Throws<SecurityException>(() => _fixture.UnixOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions));
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForCredentialManagerFileData))]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForCredentialManagerFile(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                // Skip test when group and others have no permissions
                return;
            }

            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => _fixture.UnixOperations.WriteAllText(filePath, "test", SFCredentialManagerFileImpl.Instance.ValidateFilePermissions));
        }

        [SFTheory(SkipCondition.SkipOnWindows)]
        [MemberData(nameof(TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForLogFileData))]
        public void TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForLogFile(FileAccessPermissions userPermissions,
            FileAccessPermissions groupPermissions,
            FileAccessPermissions othersPermissions)
        {
            if (groupPermissions == 0 && othersPermissions == 0)
            {
                // Skip test when group and others have no permissions
                return;
            }
            var content = "random text";
            var filePath = CreateConfigTempFile(_fixture.WorkingDirectory, content);

            var filePermissions = userPermissions | groupPermissions | othersPermissions;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => _fixture.UnixOperations.WriteAllText(filePath, "test", EasyLoggerValidator.Instance.ValidateLogFilePermissions));
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestCreateFileWithUserRwPermissions()
        {
            // arrange
            var filePath = Path.Combine(_fixture.WorkingDirectory, "testfile");

            // act
            _fixture.UnixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

            // assert
            var fileInfo = new UnixFileInfo(filePath);
            var result = _fixture.UnixOperations.CheckFileHasAnyOfPermissions(fileInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));
            Assert.False(result);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestCreateDirectoryWithUserRwxPermissions()
        {
            // arrange
            var dirPath = Path.Combine(_fixture.WorkingDirectory, "testdir");

            // act
            _fixture.UnixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // assert
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = _fixture.UnixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);
            Assert.False(result);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestNestedDir()
        {
            // arrange
            var dirPath = Path.Combine(_fixture.WorkingDirectory, "testdir", "a", "b", "c");
            _fixture.UnixOperations.CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);

            // act
            var dirInfo = new UnixDirectoryInfo(dirPath);
            var result = _fixture.UnixOperations.CheckFileHasAnyOfPermissions(dirInfo.FileAccessPermissions, FileAccessPermissions.AllPermissions & ~FileAccessPermissions.UserReadWriteExecute);

            // assert
            Assert.False(result);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestReadBytesFromEmptyFile()
        {
            // arrange
            var filePath = Path.Combine(_fixture.WorkingDirectory, $"empty_file_{Path.GetRandomFileName()}");
            _fixture.UnixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

            // act
            var bytes = _fixture.UnixOperations.ReadAllBytes(filePath, s => { });

            // assert
            Assert.Empty(bytes);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestReadBytesFromSmallFile()
        {
            // arrange
            var randomBytes = TestDataGenarator.NextBytes(19);
            var filePath = Path.Combine(_fixture.WorkingDirectory, $"small_file_{Path.GetRandomFileName()}");
            _fixture.UnixOperations.CreateFileWithPermissions(filePath, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            _fixture.UnixOperations.WriteAllBytes(filePath, randomBytes, _ => { });

            // act
            var bytes = _fixture.UnixOperations.ReadAllBytes(filePath, s => { });

            // assert
            Assert.Equal(randomBytes, bytes);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestReadBytesFromLargeFile()
        {
            // arrange
            var filePath = Path.Combine("crl", "DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl");
            var expectedBytes = File.ReadAllBytes(filePath);

            // act
            var bytes = _fixture.UnixOperations.ReadAllBytes(filePath, s => { });

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

        public static IEnumerable<object[]> TestDetectGroupOrOthersWritablePermissionsData() =>
            from a in GroupOrOthersWritablePermissions()
            from b in GroupNotWritablePermissions()
            from c in OtherNotWritablePermissions()
            select new object[] { a, b, c };

        public static IEnumerable<object[]> TestDetectGroupOrOthersNotWritablePermissionsData() =>
            from a in UserPermissions()
            from b in GroupNotWritablePermissions()
            from c in OtherNotWritablePermissions()
            select new object[] { a, b, c };

        public static IEnumerable<object[]> UserAllowedPermissionsData() =>
            UserAllowedPermissions().Select(x => new object[] { x });

        public static IEnumerable<object[]> UserAllowedWritePermissionsData() =>
            UserAllowedWritePermissions().Select(x => new object[] { x });

        public static IEnumerable<FileAccessPermissions> UserPermissionsWithRead()
        {
            yield return FileAccessPermissions.UserRead;
            yield return FileAccessPermissions.UserReadWriteExecute;
        }

        public static IEnumerable<FileAccessPermissions> UserPermissionsWithReadWrite()
        {
            yield return FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
            yield return FileAccessPermissions.UserReadWriteExecute;
        }

        public static IEnumerable<object[]> TestFailIfGroupOrOthersHavePermissionsToFileWithTomlConfigurationValidationsData() =>
            from a in UserPermissionsWithRead()
            from b in GroupPermissions()
            from c in OthersPermissions()
            select new object[] { a, b, c };

        public static IEnumerable<object[]> TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForCredentialManagerFileData() =>
            from a in UserPermissionsWithReadWrite()
            from b in GroupPermissions()
            from c in OthersPermissions()
            select new object[] { a, b, c };

        public static IEnumerable<object[]> TestFailIfGroupOrOthersHavePermissionsToFileWhileWritingWithUnixValidationsForLogFileData() =>
            from a in UserPermissionsWithReadWrite()
            from b in GroupPermissions()
            from c in OthersPermissions()
            select new object[] { a, b, c };
    }
}
