using System;
using System.IO;
using System.Security;
using Mono.Unix;
using Moq;
using Xunit;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public class SFCredentialManagerFileImplTest : SFBaseCredentialManagerTest
    {
        public SFCredentialManagerFileImplTest()
        {
            SetUp();
        }

        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<DirectoryOperations> t_directoryOperations;

        [ThreadStatic]
        private static Mock<UnixOperations> t_unixOperations;

        [ThreadStatic]
        private static Mock<IEnvironmentFacade> t_environmentOperations;

        private const string CustomJsonDir = "testdirectory";

        private static readonly string s_customJsonPath = Path.Combine(CustomJsonDir, SFCredentialManagerFileStorage.CredentialCacheFileName);

        private static readonly string s_customLockPath = Path.Combine(CustomJsonDir, SFCredentialManagerFileStorage.CredentialCacheLockName);

        private const int UserId = 1;

        private void SetUp()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_unixOperations = new Mock<UnixOperations>();
            t_environmentOperations = new Mock<IEnvironmentFacade>();
            _credentialManager = SFCredentialManagerFileImpl.Instance;
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestThatThrowsErrorWhenCacheFailToCreateCacheFile()
        {
            // arrange
            t_directoryOperations
                .Setup(d => d.Exists(s_customJsonPath))
                .Returns(false);
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_customJsonPath,
                    FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite, true))
                .Throws<IOException>();
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(CustomJsonDir);
            t_directoryOperations
                .Setup(d => d.GetParentDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryInformation(true, DateTime.UtcNow));
            t_unixOperations
                .Setup(u => u.GetDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryUnixInformation(CustomJsonDir, true, FileAccessPermissions.UserReadWriteExecute, UserId));
            t_unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(UserId);
            t_directoryOperations
                .Setup(d => d.GetDirectoryInfo(s_customLockPath))
                .Returns(new DirectoryInformation(false, null));
            t_unixOperations
                .Setup(u => u.CreateDirectoryWithPermissionsMkdir(s_customLockPath, FileAccessPermissions.UserRead))
                .Returns(0);
            _credentialManager = new SFCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object);

            // act
            var thrown = Assert.Throws<Exception>(() => _credentialManager.SaveCredentials("key", "token"));

            // assert
            Assert.Contains("Failed to create the JSON token cache file", thrown.Message);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestThatThrowsErrorWhenCacheFileCanBeAccessedByOthers()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                DirectoryOperations.Instance.CreateDirectory(tempDirectory);
                UnixOperations.Instance.CreateFileWithPermissions(Path.Combine(tempDirectory, SFCredentialManagerFileStorage.CredentialCacheFileName), FileAccessPermissions.AllPermissions);

                // act
                var thrown = Assert.Throws<SecurityException>(() => _credentialManager.SaveCredentials("key", "token"));

                // assert
                Assert.Contains("Attempting to read or write a file with too broad permissions assigned", thrown.Message);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestThatJsonFileIsCheckedIfAlreadyExists()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_customJsonPath,
                    FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite, true));
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(CustomJsonDir);
            t_fileOperations
                .SetupSequence(f => f.Exists(s_customJsonPath))
                .Returns(false)
                .Returns(true);
            t_directoryOperations
                .Setup(d => d.GetParentDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryInformation(true, DateTime.UtcNow));
            t_unixOperations
                .Setup(u => u.GetDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryUnixInformation(CustomJsonDir, true, FileAccessPermissions.UserReadWriteExecute, UserId));
            t_unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(UserId);
            t_directoryOperations
                .Setup(d => d.GetDirectoryInfo(s_customLockPath))
                .Returns(new DirectoryInformation(false, null));
            _credentialManager = new SFCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object);

            // act
            _credentialManager.SaveCredentials("key", "token");

            // assert
            t_fileOperations.Verify(f => f.Exists(s_customJsonPath), Times.Exactly(2));
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestWritingIsUnavailableIfFailedToCreateDirLock()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(CustomJsonDir);
            t_fileOperations
                .SetupSequence(f => f.Exists(s_customJsonPath))
                .Returns(false)
                .Returns(true);
            t_directoryOperations
                .Setup(d => d.GetDirectoryInfo(s_customLockPath))
                .Returns(new DirectoryInformation(false, null));
            t_directoryOperations
                .Setup(d => d.GetParentDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryInformation(true, DateTime.UtcNow));
            t_unixOperations
                .Setup(u => u.GetDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryUnixInformation(CustomJsonDir, true, FileAccessPermissions.UserReadWriteExecute, UserId));
            t_unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(UserId);
            t_unixOperations
                .Setup(u => u.CreateDirectoryWithPermissionsMkdir(s_customLockPath, SFCredentialManagerFileImpl.CredentialCacheLockDirPermissions))
                .Returns(-1);
            _credentialManager = new SFCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object);

            // act
            _credentialManager.SaveCredentials("key", "token");

            // assert
            t_fileOperations.Verify(f => f.Write(s_customJsonPath, It.IsAny<string>(), It.IsAny<Action<UnixStream>>()), Times.Never);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestReadingIsUnavailableIfFailedToCreateDirLock()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(CustomJsonDir);
            t_fileOperations
                .SetupSequence(f => f.Exists(s_customJsonPath))
                .Returns(false)
                .Returns(true);
            t_unixOperations
                .Setup(u => u.CreateDirectoryWithPermissionsMkdir(s_customLockPath, SFCredentialManagerFileImpl.CredentialCacheLockDirPermissions))
                .Returns(-1);
            t_directoryOperations
                .Setup(d => d.GetParentDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryInformation(true, DateTime.UtcNow));
            t_unixOperations
                .Setup(u => u.GetDirectoryInfo(CustomJsonDir))
                .Returns(new DirectoryUnixInformation(CustomJsonDir, true, FileAccessPermissions.UserReadWriteExecute, UserId));
            t_unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(UserId);
            t_directoryOperations
                .Setup(d => d.GetDirectoryInfo(s_customLockPath))
                .Returns(new DirectoryInformation(false, null));
            _credentialManager = new SFCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object);

            // act
            _credentialManager.GetCredentials("key");

            // assert
            t_fileOperations.Verify(f => f.ReadAllText(s_customJsonPath, It.IsAny<Action<UnixStream>>()), Times.Never);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestReadingAndWritingAreUnavailableIfDirLockExists()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                DirectoryOperations.Instance.CreateDirectory(tempDirectory);
                DirectoryOperations.Instance.CreateDirectory(Path.Combine(tempDirectory, SFCredentialManagerFileStorage.CredentialCacheLockName));

                // act
                _credentialManager.SaveCredentials("key", "token");
                var result = _credentialManager.GetCredentials("key");

                // assert
                Assert.Equal(string.Empty, result);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestThatDoesNotChangeCacheDirPermissionsWhenInsecure()
        {
            // arrange
            var insecurePermissions = FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead;
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                DirectoryOperations.Instance.CreateDirectory(tempDirectory);
                UnixOperations.Instance.ChangePermissions(tempDirectory, insecurePermissions);

                // act
                _credentialManager.SaveCredentials("key", "token");
                var result = _credentialManager.GetCredentials("key");

                // assert
                Assert.Equal("token", result);
                Assert.Equal(insecurePermissions, UnixOperations.Instance.GetDirectoryInfo(tempDirectory).Permissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestCreateDirectoryWithSecurePermissions()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetString(EnvVars.TemporaryCredentialDir))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                // act
                _credentialManager.SaveCredentials("key", "token");
                var result = _credentialManager.GetCredentials("key");

                // assert
                Assert.Equal("token", result);
                Assert.Equal(FileAccessPermissions.UserReadWriteExecute, UnixOperations.Instance.GetDirectoryInfo(tempDirectory).Permissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        private SFCredentialManagerFileImpl CreateFileCredentialManagerWithMockedEnvironmentalVariables() =>
            new(FileOperations.Instance, DirectoryOperations.Instance, UnixOperations.Instance, t_environmentOperations.Object);

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestSavingAndRemovingCredentials() => base.TestSavingAndRemovingCredentials();

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestSavingCredentialsForAnExistingKey() => base.TestSavingCredentialsForAnExistingKey();

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestRemovingCredentialsForKeyThatDoesNotExist() => base.TestRemovingCredentialsForKeyThatDoesNotExist();

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestGetCredentialsForProperKey() => base.TestGetCredentialsForProperKey();

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestGetCredentialsForTokenWithManyCharacters() => base.TestGetCredentialsForTokenWithManyCharacters();

        [SFFact(SkipCondition.SkipOnWindows)]
        public override void TestGetCredentialsForCredentialsThatDoesNotExist() => base.TestGetCredentialsForCredentialsThatDoesNotExist();
    }
}
