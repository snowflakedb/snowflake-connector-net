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

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestSavingAndRemovingCredentials() => base.TestSavingAndRemovingCredentials();

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestSavingCredentialsForAnExistingKey() => base.TestSavingCredentialsForAnExistingKey();

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestRemovingCredentialsForKeyThatDoesNotExist() => base.TestRemovingCredentialsForKeyThatDoesNotExist();

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestGetCredentialsForProperKey() => base.TestGetCredentialsForProperKey();

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestGetCredentialsForTokenWithManyCharacters() => base.TestGetCredentialsForTokenWithManyCharacters();

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public override void TestGetCredentialsForCredentialsThatDoesNotExist() => base.TestGetCredentialsForCredentialsThatDoesNotExist();
    }
}
