/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using Mono.Unix;
    using Mono.Unix.Native;
    using Moq;
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core.Tools;
    using System;
    using System.IO;
    using System.Runtime.InteropServices;

    [TestFixture]
    class SFCredentialManager
    {
        ISnowflakeCredentialManager _credentialManager;

        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<DirectoryOperations> t_directoryOperations;

        [ThreadStatic]
        private static Mock<UnixOperations> t_unixOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        private static readonly string s_expectedJsonDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private static readonly string s_expectedJsonPath = Path.Combine(s_expectedJsonDir, "temporary_credential.json");

        [SetUp] public void SetUp()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_unixOperations = new Mock<UnixOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerInMemoryImpl.Instance);
        }

        [TearDown] public void TearDown()
        {
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
        }

        private void TestCredentialManagerImplementation()
        {
            // arrange
            var key = SnowflakeCredentialManagerFactory.BuildCredentialKey("host", "user", "tokentype");
            var expectedToken = "token";

            // act
            var actualToken = _credentialManager.GetCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(actualToken));

            // act
            _credentialManager.SaveCredentials(key, expectedToken);
            actualToken = _credentialManager.GetCredentials(key);

            // assert
            Assert.AreEqual(expectedToken, actualToken);

            // act
            _credentialManager.RemoveCredentials(key);
            actualToken = _credentialManager.GetCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(actualToken));

            // act
            _credentialManager.RemoveCredentials(key);
            actualToken = _credentialManager.GetCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(actualToken));
        }

        [Test]
        public void TestUsingDefaultCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();

            // act
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.IsInstanceOf<SnowflakeCredentialManagerAdysTechImpl>(_credentialManager);
            }
            else
            {
                Assert.IsInstanceOf<SnowflakeCredentialManagerInMemoryImpl>(_credentialManager);
            }
        }

        [Test]
        public void TestSettingCustomCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerIFileImpl.Instance);

            // act
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsInstanceOf<SnowflakeCredentialManagerIFileImpl>(_credentialManager);
        }

        [Test]
        public void TestAdysTechCredentialManager()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on non-Windows");
            }

            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerAdysTechImpl.Instance);
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act & assert
            TestCredentialManagerImplementation();
        }

        [Test]
        public void TestInMemoryCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerInMemoryImpl.Instance);
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act & assert
            TestCredentialManagerImplementation();
        }

        [Test]
        public void TestJsonCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerIFileImpl.Instance);
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act & assert
            TestCredentialManagerImplementation();
        }

        [Test]
        public void TestThatThrowsErrorWhenCacheFileIsNotCreated()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            t_directoryOperations
                .Setup(d => d.Exists(s_expectedJsonDir))
                .Returns(false);
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_expectedJsonPath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR))
                .Returns(-1);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SnowflakeCredentialManagerIFileImpl.CredentialCacheDirectoryEnvironmentName))
                .Returns("testdirectory");
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SnowflakeCredentialManagerIFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object));
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act
            var thrown = Assert.Throws<Exception>(() => _credentialManager.SaveCredentials("key", "token"));

            // assert
            Assert.That(thrown.Message, Does.Contain("Failed to create the JSON token cache file"));
        }

        [Test]
        public void TestThatThrowsErrorWhenCacheFileCanBeAccessedByOthers()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_expectedJsonPath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR))
                .Returns(0);
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_expectedJsonPath))
                .Returns(FileAccessPermissions.AllPermissions);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SnowflakeCredentialManagerIFileImpl.CredentialCacheDirectoryEnvironmentName))
                .Returns("testdirectory");
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SnowflakeCredentialManagerIFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object));
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act
            var thrown = Assert.Throws<Exception>(() => _credentialManager.SaveCredentials("key", "token"));

            // assert
            Assert.That(thrown.Message, Does.Contain("Permission for the JSON token cache file should contain only the owner access"));
        }
    }
}
