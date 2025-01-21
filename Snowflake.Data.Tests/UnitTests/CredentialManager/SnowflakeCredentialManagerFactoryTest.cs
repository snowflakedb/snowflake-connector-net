using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
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

        [Test]
        public void TestUsingDefaultCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.IsInstanceOf<SFCredentialManagerWindowsNativeImpl>(credentialManager);
            }
            else
            {
                Assert.IsInstanceOf<SFCredentialManagerFileImpl>(credentialManager);
            }
        }

        [Test]
        public void TestSettingCustomCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SFCredentialManagerInMemoryImpl.Instance);

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsInstanceOf<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [Test]
        public void TestUseMemoryImplCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsInstanceOf<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [Test]
        public void TestThatThrowsErrorWhenTryingToSetCredentialManagerToNull()
        {
            // act and assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SnowflakeCredentialManagerFactory.SetCredentialManager(null));
            Assert.That(exception.Message, Does.Contain("Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method."));
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestUseWindowsCredentialManagerFailsOnUnix()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseWindowsCredentialManager);

            // assert
            Assert.AreEqual("Windows native credential manager implementation can be used only on Windows", thrown.Message);
        }

        [Test]
        [Platform("Win")]
        public void TestUseFileCredentialManagerFailsOnWindows()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseFileCredentialManager);

            // assert
            Assert.AreEqual("File credential manager implementation is not supported on Windows", thrown.Message);
        }
    }
}
