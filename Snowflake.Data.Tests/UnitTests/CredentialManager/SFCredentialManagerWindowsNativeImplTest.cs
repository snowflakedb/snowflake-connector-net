using NUnit.Framework;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    [TestFixture, NonParallelizable]
    [Platform("Win")]
    public class SFCredentialManagerWindowsNativeImplTest : SFBaseCredentialManagerTest
    {
        [SetUp]
        public void SetUp()
        {
            _credentialManager = SFCredentialManagerWindowsNativeImpl.Instance;
        }
    }
}
