using System;
using Xunit;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

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
    }
}
