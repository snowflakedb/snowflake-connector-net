using NUnit.Framework;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    [TestFixture, NonParallelizable]
    public class SFCredentialManagerInMemoryImplTest : SFBaseCredentialManagerTest
    {
        [SetUp]
        public void SetUp()
        {
            _credentialManager = SFCredentialManagerInMemoryImpl.Instance;
        }
    }
}
