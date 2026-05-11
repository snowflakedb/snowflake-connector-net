using Xunit;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public class SFCredentialManagerInMemoryImplTest : SFBaseCredentialManagerTest
    {
        public void SetUp()
        {
            _credentialManager = SFCredentialManagerInMemoryImpl.Instance;
        }
    }
}
