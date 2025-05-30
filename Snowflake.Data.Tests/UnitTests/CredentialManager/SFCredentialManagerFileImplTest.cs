using System;
using System.IO;
using System.Security;
using Mono.Unix;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    [TestFixture, NonParallelizable]
    [Platform(Exclude = "Win")]
    public class SFCredentialManagerFileImplTest : SFBaseCredentialManagerTest
    {
        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<DirectoryOperations> t_directoryOperations;

        [ThreadStatic]
        private static Mock<UnixOperations> t_unixOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        private const string CustomJsonDir = "testdirectory";

        private static readonly string s_customJsonPath = Path.Combine(CustomJsonDir, SFCredentialManagerFileStorage.CredentialCacheFileName);

        private static readonly string s_customLockPath = Path.Combine(CustomJsonDir, SFCredentialManagerFileStorage.CredentialCacheLockName);

        private const int UserId = 1;

        [SetUp]
        public void SetUp()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_unixOperations = new Mock<UnixOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            _credentialManager = SFCredentialManagerFileImpl.Instance;
        }

        [TearDown]
        public void CleanAll()
        {
            if (SFCredentialManagerFileImpl.Instance._fileStorage != null)
            {
                File.Delete(SFCredentialManagerFileImpl.Instance._fileStorage.JsonCacheFilePath);
            }
        }

        [Test]
        public void TestThatThrowsErrorWhenCacheFailToCreateCacheFile()
        {
            // arrange
            t_directoryOperations
                .Setup(d => d.Exists(s_customJsonPath))
                .Returns(false);
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_customJsonPath,
                    FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite))
                .Throws<IOException>();
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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
            Assert.That(thrown.Message, Does.Contain("Failed to create the JSON token cache file"));
        }

        [Test]
        public void TestThatThrowsErrorWhenCacheFileCanBeAccessedByOthers()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                DirectoryOperations.Instance.CreateDirectory(tempDirectory);
                UnixOperations.Instance.CreateFileWithPermissions(Path.Combine(tempDirectory, SFCredentialManagerFileStorage.CredentialCacheFileName), FileAccessPermissions.AllPermissions);

                // act
                var thrown = Assert.Throws<SecurityException>(() => _credentialManager.SaveCredentials("key", "token"));

                // assert
                Assert.That(thrown.Message, Does.Contain("Attempting to read or write a file with too broad permissions assigned"));
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [Test]
        public void TestThatJsonFileIsCheckedIfAlreadyExists()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_customJsonPath,
                    FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite));
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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

        [Test]
        public void TestWritingIsUnavailableIfFailedToCreateDirLock()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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

        [Test]
        public void TestReadingIsUnavailableIfFailedToCreateDirLock()
        {
            // arrange
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_customJsonPath))
                .Returns(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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

        [Test]
        public void TestReadingAndWritingAreUnavailableIfDirLockExists()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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
                Assert.AreEqual(string.Empty, result);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [Test]
        public void TestThatDoesNotChangeCacheDirPermissionsWhenInsecure()
        {
            // arrange
            var insecurePermissions = FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.GroupRead;
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
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
                Assert.AreEqual("token", result);
                Assert.AreEqual(insecurePermissions, UnixOperations.Instance.GetDirectoryInfo(tempDirectory).Permissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [Test]
        public void TestCreateDirectoryWithSecurePermissions()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
                .Returns(tempDirectory);
            _credentialManager = CreateFileCredentialManagerWithMockedEnvironmentalVariables();
            try
            {
                // act
                _credentialManager.SaveCredentials("key", "token");
                var result = _credentialManager.GetCredentials("key");

                // assert
                Assert.AreEqual("token", result);
                Assert.AreEqual(FileAccessPermissions.UserReadWriteExecute, UnixOperations.Instance.GetDirectoryInfo(tempDirectory).Permissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        private SFCredentialManagerFileImpl CreateFileCredentialManagerWithMockedEnvironmentalVariables() =>
            new(FileOperations.Instance, DirectoryOperations.Instance, UnixOperations.Instance, t_environmentOperations.Object);
    }
}
