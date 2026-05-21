using System;
using System.IO;
using Xunit;
using Moq;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;


namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{

    public class SFCredentialManagerFileStorageTest
    {
        private const string SnowflakeCacheLocation = "/Users/snowflake/cache";
        private const string CommonCacheLocation = "/Users/snowflake/.cache";
        private const string HomeLocation = "/Users/snowflake";

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [SetUp]
        public void SetUp()
        {
            t_environmentOperations = new Mock<EnvironmentOperations>();
        }

        [SFFact]
        public void TestChooseLocationFromSnowflakeCacheEnvironmentVariable()
        {
            // arrange
            MockSnowflakeCacheEnvironmentVariable();
            MockCommonCacheEnvironmentVariable();
            MockHomeLocation();

            // act
            var fileStorage = new SFCredentialManagerFileStorage(t_environmentOperations.Object);

            // assert
            AssertFileStorageForLocation(SnowflakeCacheLocation, fileStorage);
        }

        [SFFact]
        public void TestChooseLocationFromCommonCacheEnvironmentVariable()
        {
            // arrange
            MockCommonCacheEnvironmentVariable();
            MockHomeLocation();
            var expectedLocation = Path.Combine(CommonCacheLocation, SFCredentialManagerFileStorage.CredentialCacheDirName);

            // act
            var fileStorage = new SFCredentialManagerFileStorage(t_environmentOperations.Object);

            // assert
            AssertFileStorageForLocation(expectedLocation, fileStorage);
        }

        [SFFact]
        public void TestChooseLocationFromHomeFolder()
        {
            // arrange
            MockHomeLocation();
            var expectedLocation = Path.Combine(HomeLocation, SFCredentialManagerFileStorage.CommonCacheDirectoryName, SFCredentialManagerFileStorage.CredentialCacheDirName);

            // act
            var fileStorage = new SFCredentialManagerFileStorage(t_environmentOperations.Object);

            // assert
            AssertFileStorageForLocation(expectedLocation, fileStorage);
        }

        [SFFact]
        public void TestFailWhenLocationCannotBeIdentified()
        {
            // act
            var thrown = Assert.Throws<Exception>(() => new SFCredentialManagerFileStorage(t_environmentOperations.Object));

            // assert
            Assert.That(thrown.Message, Contains.Substring("Unable to identify credential cache directory"));
        }

        private void AssertFileStorageForLocation(string directory, SFCredentialManagerFileStorage fileStorage)
        {
            Assert.NotNull(fileStorage);
            Assert.Equal(directory, fileStorage.JsonCacheDirectory);
            Assert.Equal(Path.Combine(directory, SFCredentialManagerFileStorage.CredentialCacheFileName), fileStorage.JsonCacheFilePath);
            Assert.Equal(Path.Combine(directory, SFCredentialManagerFileStorage.CredentialCacheLockName), fileStorage.JsonCacheLockPath);
        }

        private void MockSnowflakeCacheEnvironmentVariable()
        {
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CredentialCacheDirectoryEnvironmentName))
                .Returns(SnowflakeCacheLocation);
        }

        private void MockCommonCacheEnvironmentVariable()
        {
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(SFCredentialManagerFileStorage.CommonCacheDirectoryEnvironmentName))
                .Returns(CommonCacheLocation);
        }

        private void MockHomeLocation()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns(HomeLocation);
        }
    }
}
