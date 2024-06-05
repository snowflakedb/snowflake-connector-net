/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
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

    public abstract class SFBaseCredentialManagerTest
    {
        protected ISnowflakeCredentialManager _credentialManager;

        [Test]
        public void TestSavingAndRemovingCredentials()
        {
            // arrange
            var key = "mockKey";
            var expectedToken = "token";

            // act
            _credentialManager.SaveCredentials(key, expectedToken);

            // assert
            Assert.AreEqual(expectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        [Test]
        public void TestSavingCredentialsForAnExistingKey()
        {
            // arrange
            var key = "mockKey";
            var firstExpectedToken = "mockToken1";
            var secondExpectedToken = "mockToken2";

            try
            {
                // act
                _credentialManager.SaveCredentials(key, firstExpectedToken);

                // assert
                Assert.AreEqual(firstExpectedToken, _credentialManager.GetCredentials(key));

                // act
                _credentialManager.SaveCredentials(key, secondExpectedToken);

                // assert
                Assert.AreEqual(secondExpectedToken, _credentialManager.GetCredentials(key));

                // act
                _credentialManager.RemoveCredentials(key);

                // assert
                Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
            }
            catch (Exception ex)
            {
                // assert
                Assert.Fail("Should not throw an exception: " + ex.Message);
            }
        }

        [Test]
        public void TestRemovingCredentialsForKeyThatDoesNotExist()
        {
            // arrange
            var key = "mockKey";

            try
            {
                // act
                _credentialManager.RemoveCredentials(key);

                // assert
                Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
            }
            catch (Exception ex)
            {
                // assert
                Assert.Fail("Should not throw an exception: " + ex.Message);
            }
        }
    }

    [TestFixture]
    [Platform("Win")]
    public class SFNativeCredentialManagerTest : SFBaseCredentialManagerTest
    {
        [SetUp]
        public void SetUp()
        {
            _credentialManager = SnowflakeCredentialManagerWindowsNativeImpl.Instance;
        }
    }

    [TestFixture]
    public class SFInMemoryCredentialManagerTest : SFBaseCredentialManagerTest
    {
        [SetUp]
        public void SetUp()
        {
            _credentialManager = SnowflakeCredentialManagerInMemoryImpl.Instance;
        }
    }

    [TestFixture]
    public class SFFileCredentialManagerTest : SFBaseCredentialManagerTest
    {
        [SetUp]
        public void SetUp()
        {
            _credentialManager = SnowflakeCredentialManagerFileImpl.Instance;
        }
    }

    [TestFixture]
    class SFCredentialManagerTest
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

        private static readonly string s_defaultJsonDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private const string CustomJsonDir = "testdirectory";

        private static readonly string s_defaultJsonPath = Path.Combine(s_defaultJsonDir, SnowflakeCredentialManagerFileImpl.CredentialCacheFileName);

        private static readonly string s_customJsonPath = Path.Combine(CustomJsonDir, SnowflakeCredentialManagerFileImpl.CredentialCacheFileName);

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
                Assert.IsInstanceOf<SnowflakeCredentialManagerWindowsNativeImpl>(_credentialManager);
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
            SnowflakeCredentialManagerFactory.SetCredentialManager(SnowflakeCredentialManagerFileImpl.Instance);

            // act
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsInstanceOf<SnowflakeCredentialManagerFileImpl>(_credentialManager);
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
                .Setup(d => d.Exists(s_defaultJsonDir))
                .Returns(false);
            t_unixOperations
                .Setup(u => u.CreateFileWithPermissions(s_customJsonPath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR))
                .Returns(-1);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SnowflakeCredentialManagerFileImpl.CredentialCacheDirectoryEnvironmentName))
                .Returns(CustomJsonDir);
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SnowflakeCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object));
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
                .Setup(u => u.CreateFileWithPermissions(s_defaultJsonPath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR))
                .Returns(0);
            t_unixOperations
                .Setup(u => u.GetFilePermissions(s_defaultJsonPath))
                .Returns(FileAccessPermissions.AllPermissions);
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SnowflakeCredentialManagerFileImpl.CredentialCacheDirectoryEnvironmentName))
                .Returns(CustomJsonDir);
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SnowflakeCredentialManagerFileImpl(t_fileOperations.Object, t_directoryOperations.Object, t_unixOperations.Object, t_environmentOperations.Object));
            _credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // act
            var thrown = Assert.Throws<Exception>(() => _credentialManager.SaveCredentials("key", "token"));

            // assert
            Assert.That(thrown.Message, Does.Contain("Permission for the JSON token cache file should contain only the owner access"));
        }
    }
}
