using System;
using System.Runtime.InteropServices;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    [TestFixture, NonParallelizable]
    public class SnowflakeCredentialManagerFactoryTest
    {
        [TearDown]
        public void TearDown()
        {
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
        }

        [SFFact]
        public void TestUsingDefaultCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.InstanceOf<SFCredentialManagerWindowsNativeImpl>(credentialManager);
            }
            else
            {
                Assert.InstanceOf<SFCredentialManagerFileImpl>(credentialManager);
            }
        }

        [SFFact]
        public void TestSettingCustomCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SFCredentialManagerInMemoryImpl.Instance);

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.InstanceOf<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestUseMemoryImplCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.InstanceOf<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestThatThrowsErrorWhenTryingToSetCredentialManagerToNull()
        {
            // act and assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SnowflakeCredentialManagerFactory.SetCredentialManager(null));
            Assert.That(exception.Message, Does.Contain("Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method."));
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestUseWindowsCredentialManagerFailsOnUnix()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseWindowsCredentialManager);

            // assert
            Assert.Equal("Windows native credential manager implementation can be used only on Windows", thrown.Message);
        }

        [SFFact]
        [Platform("Win")]
        public void TestUseFileCredentialManagerFailsOnWindows()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseFileCredentialManager);

            // assert
            Assert.Equal("File credential manager implementation is not supported on Windows", thrown.Message);
        }
    }
}
