using System;
using Xunit;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public class SFCredentialManagerWindowsNativeImplTest : SFBaseCredentialManagerTest{
        public SFCredentialManagerWindowsNativeImplTest()
        {
            SetUp();
        }

        public void SetUp()
        {
            _credentialManager = SFCredentialManagerWindowsNativeImpl.Instance;
        }

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestSavingAndRemovingCredentials() => base.TestSavingAndRemovingCredentials();

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestSavingCredentialsForAnExistingKey() => base.TestSavingCredentialsForAnExistingKey();

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestRemovingCredentialsForKeyThatDoesNotExist() => base.TestRemovingCredentialsForKeyThatDoesNotExist();

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestGetCredentialsForProperKey() => base.TestGetCredentialsForProperKey();

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestGetCredentialsForTokenWithManyCharacters() => base.TestGetCredentialsForTokenWithManyCharacters();

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public override void TestGetCredentialsForCredentialsThatDoesNotExist() => base.TestGetCredentialsForCredentialsThatDoesNotExist();
    }
}
