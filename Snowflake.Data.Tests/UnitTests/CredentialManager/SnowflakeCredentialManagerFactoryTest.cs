using System;
using System.Runtime.InteropServices;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public class SnowflakeCredentialManagerFactoryTest : IDisposable
    {
        private void TearDown()
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
                Assert.IsType<SFCredentialManagerWindowsNativeImpl>(credentialManager);
            }
            else
            {
                Assert.IsType<SFCredentialManagerFileImpl>(credentialManager);
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
            Assert.IsType<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestUseMemoryImplCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsType<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestThatThrowsErrorWhenTryingToSetCredentialManagerToNull()
        {
            // act and assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SnowflakeCredentialManagerFactory.SetCredentialManager(null));
            Assert.Contains("Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method.", exception.Message);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestUseWindowsCredentialManagerFailsOnUnix()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseWindowsCredentialManager);

            // assert
            Assert.Equal("Windows native credential manager implementation can be used only on Windows", thrown.Message);
        }

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestUseFileCredentialManagerFailsOnWindows()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseFileCredentialManager);

            // assert
            Assert.Equal("File credential manager implementation is not supported on Windows", thrown.Message);
        }

        public void Dispose()
        {
            TearDown();
        }
    }
}
